// FriendListChatserver.js
const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");
const admin = require("firebase-admin");
const seventhServiceAccount = require("./seventhServiceAccountKey.json");

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  pingInterval: 10000,
  pingTimeout: 15000,
  maxHttpBufferSize: 20 * 1024 * 1024, // 20MB buffer for voice/avatar/media fallbacks
  transports: ["websocket", "polling"],
});

const PORT = process.env.PORT || 4000;
const OFFLINE_MSG_TTL_MS = 24 * 60 * 60 * 1000; // 24 Hours
const SWEEP_INTERVAL_MS = 10 * 60 * 1000; // Run 24h cleanup every 10 minutes
const HEARTBEAT_DROP_MS = 14000; // 14s without ping = lost internet

// ==============================================================
// 🔥 FIREBASE ADMIN INIT FOR `seventhDb` (24-HOUR AUTO-DELETE SWEEPER)
// ==============================================================
let seventhFirestore = null;

try {
  const seventhApp = admin.initializeApp(
    {
      credential: admin.credential.cert(seventhServiceAccount),
    },
    "seventhDbAdmin"
  );
  seventhFirestore = admin.firestore(seventhApp);
  console.log("✅ [7thDB] Firebase Admin connected via seventhServiceAccountKey.json.");
} catch (err) {
  console.error("❌ [7thDB] Failed to initialize Firebase Admin:", err.message);
}

// Track rooms that recently received offline messages as a fallback if collectionGroup index isn't created yet
const activeOfflineRoomIds = new Set();

async function sweepExpiredFirestoreMessages() {
  if (!seventhFirestore) return { deleted: 0, skipped: true };

  const now = Date.now();
  const cutoff24h = now - OFFLINE_MSG_TTL_MS;
  let totalDeleted = 0;

  // Primary Strategy: Fast collectionGroup('messages') query across all FriendListChat rooms
  try {
    let hasMore = true;
    while (hasMore) {
      const snap = await seventhFirestore
        .collectionGroup("messages")
        .where("expiresAt", "<=", now)
        .limit(400)
        .get();

      if (snap.empty) {
        hasMore = false;
        break;
      }

      const batch = seventhFirestore.batch();
      snap.docs.forEach((docSnap) => batch.delete(docSnap.ref));
      await batch.commit();
      totalDeleted += snap.size;

      if (snap.size < 400) hasMore = false;
    }
  } catch (err) {
    // Fallback Strategy: Scan FriendListChat rooms directly if collectionGroup index isn't created yet
    console.warn(
      "⚠️ [24h Sweeper] collectionGroup fallback (create single-field exemption index on `messages.expiresAt` if needed):",
      err.message
    );

    try {
      const roomRefs = await seventhFirestore
        .collection("FriendListChat")
        .listDocuments();

      const allRoomIds = new Set([
        ...roomRefs.map((r) => r.id),
        ...Array.from(activeOfflineRoomIds),
      ]);

      for (const roomId of allRoomIds) {
        const msgSnap = await seventhFirestore
          .collection("FriendListChat")
          .doc(roomId)
          .collection("messages")
          .get();

        if (msgSnap.empty) {
          activeOfflineRoomIds.delete(roomId);
          continue;
        }

        const expiredDocs = msgSnap.docs.filter((d) => {
          const data = d.data() || {};
          const exp = Number(data.expiresAt) || 0;
          const ts = Number(data.timestamp) || 0;
          return (exp > 0 && exp <= now) || (ts > 0 && ts <= cutoff24h);
        });

        if (expiredDocs.length > 0) {
          for (let i = 0; i < expiredDocs.length; i += 400) {
            const slice = expiredDocs.slice(i, i + 400);
            const batch = seventhFirestore.batch();
            slice.forEach((d) => batch.delete(d.ref));
            await batch.commit();
            totalDeleted += slice.length;
          }
        }
      }
    } catch (fallbackErr) {
      console.error("❌ [24h Sweeper] Fallback sweep error:", fallbackErr.message);
    }
  }

  if (totalDeleted > 0) {
    console.log(`🧹 [24h Sweeper] Deleted ${totalDeleted} expired offline message(s).`);
  }
  return { deleted: totalDeleted, timestamp: now };
}

// Run initial sweep 15s after boot, then every 10 minutes
setTimeout(() => sweepExpiredFirestoreMessages().catch(() => {}), 15000);
setInterval(() => sweepExpiredFirestoreMessages().catch(() => {}), SWEEP_INTERVAL_MS);

// ==============================================================
// 🧠 IN-MEMORY LIVE STATE (ZERO FIRESTORE LOAD FOR SIGNALING)
// ==============================================================
const onlineUsers = new Map(); // uid -> user record
const emailToUid = new Map(); // emailLower -> uid
const socketToUid = new Map(); // socketId -> uid
const rooms = new Map(); // roomId -> { members: Set<uid>, activeThemeId, memberStates: Map }

function getOrCreateRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, {
      members: new Set(),
      activeThemeId: null,
      memberStates: new Map(),
    });
  }
  return rooms.get(roomId);
}

function getUserByUidOrEmail(uid, email) {
  if (uid && onlineUsers.has(uid)) return onlineUsers.get(uid);
  if (email) {
    const mappedUid = emailToUid.get(String(email).toLowerCase());
    if (mappedUid && onlineUsers.has(mappedUid)) {
      return onlineUsers.get(mappedUid);
    }
  }
  return null;
}

function emitToUser(uid, email, event, payload) {
  const user = getUserByUidOrEmail(uid, email);
  if (user && user.socketId) {
    io.to(user.socketId).emit(event, payload);
    return true;
  }
  return false;
}

function getPartnerInRoom(roomId, myUid) {
  const room = rooms.get(roomId);
  if (!room) return null;
  for (const memberUid of room.members) {
    if (memberUid !== myUid) {
      return onlineUsers.get(memberUid) || { uid: memberUid };
    }
  }
  return null;
}

function handleUserLeavingRoom(uid, roomId, reason = "left_chat") {
  const user = onlineUsers.get(uid);
  const targetRoomId = roomId || user?.currentRoomId;
  if (!targetRoomId) return;

  const room = rooms.get(targetRoomId);
  const targetUid = user?.targetUid || getPartnerInRoom(targetRoomId, uid)?.uid || null;

  if (room) {
    room.members.delete(uid);
    room.memberStates.delete(uid);
    if (room.members.size === 0) {
      rooms.delete(targetRoomId);
    }
  }

  if (user && user.currentRoomId === targetRoomId) {
    user.currentRoomId = null;
    user.targetUid = null;
    user.inCall = false;
  }

  const watchPayload = {
    roomId: targetRoomId,
    watcherUid: uid,
    targetUid,
    isWatching: false,
    reason,
    timestamp: Date.now(),
  };

  if (targetUid) {
    emitToUser(targetUid, null, "friend_watch_status", watchPayload);
    if (reason === "lost_internet") {
      emitToUser(targetUid, null, "friend_partner_lost_internet", {
        roomId: targetRoomId,
        droppedUid: uid,
        reason,
        message: "User lost connection.",
        timestamp: Date.now(),
      });
    } else {
      emitToUser(targetUid, null, "friend_partner_left_chat", {
        roomId: targetRoomId,
        droppedUid: uid,
        reason,
        timestamp: Date.now(),
      });
    }
  }

  io.to(targetRoomId).emit("friend_watch_status", watchPayload);
}

// ==============================================================
// 💓 HEARTBEAT & SILENT INTERNET-DROP MONITOR (RUNS EVERY 3 SECONDS)
// ==============================================================
setInterval(() => {
  const now = Date.now();
  for (const [uid, user] of onlineUsers.entries()) {
    if (!user.currentRoomId) continue;
    if (user.protectedUntil && now < user.protectedUntil) continue;

    if (now - (user.lastPing || 0) > HEARTBEAT_DROP_MS) {
      const droppedRoomId = user.currentRoomId;
      console.log(`⚠️ [Heartbeat Timeout] User ${uid} lost connection in room ${droppedRoomId}`);
      handleUserLeavingRoom(uid, droppedRoomId, "lost_internet");
    }
  }
}, 3000);

// ==============================================================
// 🌐 HTTP HEALTH & MANUAL SWEEP ENDPOINTS
// ==============================================================
app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "FriendListChatserver",
    onlineUsers: onlineUsers.size,
    activeRooms: rooms.size,
    seventhDbConnected: Boolean(seventhFirestore),
    uptimeSec: Math.round(process.uptime()),
  });
});

app.post("/cleanup-expired", async (req, res) => {
  const result = await sweepExpiredFirestoreMessages();
  res.json({ ok: true, ...result });
});

// ==============================================================
// 🔌 SOCKET.IO EVENT HANDLERS
// ==============================================================
io.on("connection", (socket) => {
  // 1. Register user upon app open / login / reconnect
  socket.on("friend_register_user", (data = {}, ack) => {
    const { uid, email, name, image, isVip } = data;
    if (!uid) {
      ack?.({ ok: false, error: "uid required" });
      return;
    }

    const emailLower = email ? String(email).toLowerCase() : null;
    const prev = onlineUsers.get(uid);

    if (prev && prev.socketId && prev.socketId !== socket.id) {
      socketToUid.delete(prev.socketId);
    }

    const userRecord = {
      socketId: socket.id,
      uid,
      email: emailLower,
      name: name || prev?.name || "User",
      image: image || prev?.image || null,
      isVip: Boolean(isVip ?? prev?.isVip),
      currentRoomId: prev?.currentRoomId || null,
      targetUid: prev?.targetUid || null,
      inCall: Boolean(prev?.inCall),
      protectedUntil: prev?.protectedUntil || 0,
      lastPing: Date.now(),
    };

    onlineUsers.set(uid, userRecord);
    socketToUid.set(socket.id, uid);
    if (emailLower) emailToUid.set(emailLower, uid);

    if (userRecord.currentRoomId) {
      socket.join(userRecord.currentRoomId);
    }

    ack?.({ ok: true, onlineCount: onlineUsers.size });
  });

  // 2. Query who is currently waiting in chat rooms with me (Powers My Friends "Live 👀" tab)
  socket.on("friend_get_live_watchers", (data = {}, ack) => {
    const myUid = data.uid || socketToUid.get(socket.id);
    if (!myUid) {
      ack?.({ watchers: [] });
      return;
    }

    const watchers = [];
    for (const [uid, u] of onlineUsers.entries()) {
      if (uid !== myUid && u.targetUid === myUid && u.currentRoomId) {
        watchers.push({
          watcherUid: u.uid,
          email: u.email,
          name: u.name,
          image: u.image,
          roomId: u.currentRoomId,
          isVip: u.isVip,
          updatedAt: u.lastPing,
        });
      }
    }
    ack?.({ watchers });
  });

  // 3. Join Friend Chat Room + Publish Live Watcher Presence
  socket.on("friend_watch_join", (data = {}, ack) => {
    const {
      roomId,
      watcherUid,
      targetUid,
      targetEmail,
      name,
      email,
      image,
      isVip,
      country,
      timeZone,
      privacy,
      dnd,
    } = data;

    const uid = watcherUid || socketToUid.get(socket.id);
    if (!uid || !roomId) {
      ack?.({ ok: false });
      return;
    }

    const emailLower = email ? String(email).toLowerCase() : null;
    const resolvedTargetUser = getUserByUidOrEmail(targetUid, targetEmail);
    const resolvedTargetUid = targetUid || resolvedTargetUser?.uid || null;

    const existing = onlineUsers.get(uid) || {};
    if (existing.currentRoomId && existing.currentRoomId !== roomId) {
      socket.leave(existing.currentRoomId);
      handleUserLeavingRoom(uid, existing.currentRoomId, "switched_room");
    }

    const userRecord = {
      ...existing,
      socketId: socket.id,
      uid,
      email: emailLower || existing.email || null,
      name: name || existing.name || "User",
      image: image || existing.image || null,
      isVip: Boolean(isVip ?? existing.isVip),
      currentRoomId: roomId,
      targetUid: resolvedTargetUid,
      lastPing: Date.now(),
    };

    onlineUsers.set(uid, userRecord);
    socketToUid.set(socket.id, uid);
    if (userRecord.email) emailToUid.set(userRecord.email, uid);

    socket.join(roomId);
    const room = getOrCreateRoom(roomId);
    room.members.add(uid);
    room.memberStates.set(uid, {
      uid,
      email: userRecord.email,
      name: userRecord.name,
      image: userRecord.image,
      isVip: userRecord.isVip,
      country: country || "INDIA",
      timeZone: timeZone || null,
      privacy: privacy !== undefined ? Boolean(privacy) : true,
      dnd: Boolean(dnd),
      joinedAt: Date.now(),
    });

    const watchPayload = {
      roomId,
      watcherUid: uid,
      targetUid: resolvedTargetUid,
      name: userRecord.name,
      email: userRecord.email,
      image: userRecord.image,
      isVip: userRecord.isVip,
      isWatching: true,
      timestamp: Date.now(),
    };

    socket.to(roomId).emit("friend_watch_status", watchPayload);
    if (resolvedTargetUid) {
      emitToUser(resolvedTargetUid, targetEmail, "friend_watch_status", watchPayload);
    }

    const partnerInRoom = getPartnerInRoom(roomId, uid);
    const partnerState = partnerInRoom
      ? room.memberStates.get(partnerInRoom.uid) || null
      : null;

    if (partnerInRoom) {
      socket.emit("friend_watch_status", {
        roomId,
        watcherUid: partnerInRoom.uid,
        targetUid: uid,
        name: partnerInRoom.name,
        email: partnerInRoom.email,
        image: partnerInRoom.image,
        isVip: partnerInRoom.isVip,
        isWatching: true,
        timestamp: Date.now(),
      });
    }

    ack?.({
      ok: true,
      partnerInRoom: Boolean(partnerInRoom),
      partnerState,
      activeThemeId: room.activeThemeId || null,
    });
  });

  // 4. Explicit Leave Room (Back button / Background / Unmount)
  socket.on("friend_user_left_chat", (data = {}) => {
    const uid = data.watcherUid || data.senderUid || socketToUid.get(socket.id);
    if (!uid) return;
    const roomId = data.roomId || onlineUsers.get(uid)?.currentRoomId;
    if (roomId) socket.leave(roomId);
    handleUserLeavingRoom(uid, roomId, data.reason || "left_chat");
  });

  // 5. Heartbeat Ping (Every 2s while inside FriendListCallService)
  socket.on("friend_ping", (data = {}) => {
    const uid = data.uid || socketToUid.get(socket.id);
    if (!uid) return;
    const user = onlineUsers.get(uid);
    if (user) {
      user.lastPing = Date.now();
      user.socketId = socket.id;
      if (data.inCall !== undefined) user.inCall = Boolean(data.inCall);
      if (data.roomId && user.currentRoomId !== data.roomId) {
        user.currentRoomId = data.roomId;
        socket.join(data.roomId);
      }
    }
  });

  // 6. Gallery / Camera / Call Setup Protection
  socket.on("friend_set_protection", (data = {}) => {
    const uid = data.uid || socketToUid.get(socket.id);
    const user = onlineUsers.get(uid);
    if (!user) return;
    const durationMs = Number(data.durationMs) || 25000;
    user.protectedUntil = data.active ? Date.now() + durationMs : 0;
    if (data.roomId) {
      socket.to(data.roomId).emit("friend_call_signal", {
        roomId: data.roomId,
        senderUid: uid,
        signalType: "PICKING_IMAGE",
        payload: Boolean(data.active),
      });
    }
  });

  // 7. In-Call Status Sync
  socket.on("friend_in_call_status", (data = {}) => {
    const uid = data.uid || socketToUid.get(socket.id);
    const user = onlineUsers.get(uid);
    if (user) user.inCall = Boolean(data.inCall);
  });

  // 8. Room Wipe / Lost Internet Notify
  socket.on("friend_room_wipe", (data = {}) => {
    const uid = data.senderUid || socketToUid.get(socket.id);
    const roomId = data.roomId || onlineUsers.get(uid)?.currentRoomId;
    if (uid && roomId) {
      handleUserLeavingRoom(uid, roomId, data.reason || "lost_internet");
    }
  });

  // ==============================================================
  // 📡 CHAT WEBRTC SIGNALING (ZERO FIRESTORE READS/WRITES)
  // ==============================================================
  socket.on("friend_chat_webrtc_offer", (data = {}) => {
    const { roomId, offer, offerId, role, senderUid, senderEmail } = data;
    if (!roomId || !offer) return;
    socket.to(roomId).emit("friend_chat_webrtc_offer", {
      roomId,
      offer,
      offerId,
      role,
      senderUid: senderUid || socketToUid.get(socket.id),
      senderEmail,
    });
  });

  socket.on("friend_chat_webrtc_answer", (data = {}) => {
    const { roomId, answer, answerId, forOfferId, role, senderUid } = data;
    if (!roomId || !answer) return;
    socket.to(roomId).emit("friend_chat_webrtc_answer", {
      roomId,
      answer,
      answerId,
      forOfferId,
      role,
      senderUid: senderUid || socketToUid.get(socket.id),
    });
  });

  socket.on("friend_chat_webrtc_ice", (data = {}) => {
    const { roomId, candidate, role, senderUid } = data;
    if (!roomId || !candidate) return;
    socket.to(roomId).emit("friend_chat_webrtc_ice", {
      roomId,
      candidate,
      role,
      senderUid: senderUid || socketToUid.get(socket.id),
    });
  });

  socket.on("friend_chat_ice_restart", (data = {}) => {
    const { roomId } = data;
    if (!roomId) return;
    socket.to(roomId).emit("friend_chat_ice_restart", {
      ...data,
      senderUid: data.senderUid || socketToUid.get(socket.id),
    });
  });

  // ==============================================================
  // 📞 AUDIO & VIDEO CALL SIGNALING + LIVE FEATURES SYNC
  // ==============================================================
  socket.on("friend_call_signal", (data = {}) => {
    const { roomId, targetUid, targetEmail, signalType, payload, senderUid, senderEmail } = data;
    if (!roomId || !signalType) return;

    const uid = senderUid || socketToUid.get(socket.id);
    const packet = {
      roomId,
      senderUid: uid,
      senderEmail,
      targetUid,
      signalType,
      payload,
      timestamp: Date.now(),
    };

    socket.to(roomId).emit("friend_call_signal", packet);

    if (targetUid || targetEmail) {
      const targetUser = getUserByUidOrEmail(targetUid, targetEmail);
      if (targetUser && targetUser.currentRoomId !== roomId) {
        io.to(targetUser.socketId).emit("friend_call_signal", packet);
      }
    }
  });

  socket.on("friend_call_media_sync", (data = {}) => {
    const { roomId } = data;
    if (!roomId) return;
    const uid = data.senderUid || socketToUid.get(socket.id);

    const room = rooms.get(roomId);
    if (room && uid) {
      const prevState = room.memberStates.get(uid) || {};
      room.memberStates.set(uid, { ...prevState, ...data, updatedAt: Date.now() });
      if (data.themeId) {
        room.activeThemeId = data.themeId;
      }
    }

    socket.to(roomId).emit("friend_call_media_sync", {
      ...data,
      senderUid: uid,
      timestamp: Date.now(),
    });
  });

  // ==============================================================
  // 💬 HYBRID MESSAGE ROUTER (SOCKET WHEN IN ROOM, FIRESTORE ONLY WHEN OFFLINE)
  // ==============================================================
  socket.on("friend_hybrid_message", (data = {}, ack) => {
    const { roomId, targetUid, targetEmail, eventType = "MESSAGE", payload } = data;
    const senderUid = data.senderUid || socketToUid.get(socket.id);
    if (!roomId) {
      ack?.({ ok: false, deliveredVia: "offline", inRoom: false });
      return;
    }

    const room = rooms.get(roomId);
    const targetUser = getUserByUidOrEmail(targetUid, targetEmail);
    const isTargetInRoom = Boolean(
      targetUser &&
        targetUser.currentRoomId === roomId &&
        room?.members.has(targetUser.uid)
    );

    if (isTargetInRoom) {
      io.to(targetUser.socketId).emit("friend_hybrid_message", {
        roomId,
        senderUid,
        senderEmail: data.senderEmail,
        eventType,
        payload,
        timestamp: Date.now(),
      });
      ack?.({ ok: true, deliveredVia: "socket", inRoom: true });
      return;
    }

    activeOfflineRoomIds.add(roomId);

    if (targetUser && targetUser.socketId) {
      io.to(targetUser.socketId).emit("friend_unread_notification", {
        roomId,
        fromUid: senderUid,
        fromEmail: data.senderEmail,
        timestamp: Date.now(),
      });
    }

    ack?.({ ok: true, deliveredVia: "offline", inRoom: false });
  });

  // Live theme / reaction / typing / voice-translate / gift / privacy events
  socket.on("friend_chat_event", (data = {}) => {
    const { roomId, eventType, payload } = data;
    if (!roomId || !eventType) return;
    const uid = data.senderUid || socketToUid.get(socket.id);

    if (eventType === "THEME_UPDATE" && payload?.themeId) {
      const room = getOrCreateRoom(roomId);
      room.activeThemeId = payload.themeId;
    }

    socket.to(roomId).emit("friend_chat_event", {
      roomId,
      senderUid: uid,
      senderEmail: data.senderEmail,
      eventType,
      payload,
      timestamp: Date.now(),
    });
  });

  // ==============================================================
  // 🗑️ INSTANT BACKEND DELETE OF SEEN FIRESTORE MESSAGES
  // ==============================================================
  socket.on("friend_mark_firestore_seen", async (data = {}) => {
    const { roomId, messageIds } = data;
    if (!seventhFirestore || !roomId || !Array.isArray(messageIds) || messageIds.length === 0) {
      return;
    }
    try {
      const batch = seventhFirestore.batch();
      const colRef = seventhFirestore
        .collection("FriendListChat")
        .doc(roomId)
        .collection("messages");

      messageIds.slice(0, 400).forEach((id) => {
        if (id) batch.delete(colRef.doc(String(id)));
      });
      await batch.commit();
    } catch (e) {}
  });

  // ==============================================================
  // 🔌 SOCKET DISCONNECT CLEANUP
  // ==============================================================
  socket.on("disconnect", (reason) => {
    const uid = socketToUid.get(socket.id);
    socketToUid.delete(socket.id);
    if (!uid) return;

    const user = onlineUsers.get(uid);
    if (!user || user.socketId !== socket.id) return;

    const now = Date.now();
    if (user.protectedUntil && now < user.protectedUntil) {
      const waitMs = Math.min(12000, user.protectedUntil - now + 1000);
      setTimeout(() => {
        const latest = onlineUsers.get(uid);
        if (latest && latest.socketId === socket.id) {
          handleUserLeavingRoom(uid, latest.currentRoomId, "lost_internet");
          onlineUsers.delete(uid);
        }
      }, waitMs);
      return;
    }

    if (user.currentRoomId) {
      const dropReason =
        reason === "client namespace disconnect" ? "left_chat" : "lost_internet";
      handleUserLeavingRoom(uid, user.currentRoomId, dropReason);
    }

    onlineUsers.delete(uid);
  });
});

server.listen(PORT, () => {
  console.log(`🚀 FriendListChatserver running on port ${PORT}`);
});