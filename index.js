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
  pingInterval: 15000,
  pingTimeout: 30000,
  maxHttpBufferSize: 20 * 1024 * 1024,
  transports: ["websocket", "polling"],
});

const PORT = process.env.PORT || 4000;
const OFFLINE_MSG_TTL_MS = 24 * 60 * 60 * 1000; // 24 Hours
const SWEEP_INTERVAL_MS = 10 * 60 * 1000; // Every 10 minutes
const HEARTBEAT_DROP_MS = 45000; // 45s safety ceiling (only if socket is ALSO dead)
const CHAT_DISCONNECT_GRACE_MS = 10000; // 10s reconnect grace in chat
const CALL_DISCONNECT_GRACE_MS = 18000; // 18s reconnect grace during active call

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

const activeOfflineRoomIds = new Set();

async function sweepExpiredFirestoreMessages() {
  if (!seventhFirestore) {
    return { deletedMessages: 0, deletedRooms: 0, skipped: true };
  }

  const now = Date.now();
  const cutoff24h = now - OFFLINE_MSG_TTL_MS;
  let deletedMessages = 0;
  let deletedRooms = 0;

  try {
    const roomRefs = await seventhFirestore
      .collection("FriendListChat")
      .listDocuments();

    const allRoomIds = new Set([
      ...roomRefs.map((r) => r.id),
      ...Array.from(activeOfflineRoomIds),
    ]);

    for (const roomId of allRoomIds) {
      const roomDocRef = seventhFirestore
        .collection("FriendListChat")
        .doc(roomId);
      const messagesColRef = roomDocRef.collection("messages");
      const msgSnap = await messagesColRef.get();

      if (msgSnap.empty) {
        await roomDocRef.delete().catch(() => {});
        activeOfflineRoomIds.delete(roomId);
        deletedRooms += 1;
        continue;
      }

      const expiredDocs = [];
      let remainingCount = 0;

      msgSnap.docs.forEach((d) => {
        const data = d.data() || {};
        const exp = Number(data.expiresAt) || 0;
        const ts = Number(data.timestamp) || 0;
        const isExpired =
          (exp > 0 && exp <= now) || (ts > 0 && ts <= cutoff24h);

        if (isExpired) {
          expiredDocs.push(d);
        } else {
          remainingCount += 1;
        }
      });

      if (expiredDocs.length > 0) {
        for (let i = 0; i < expiredDocs.length; i += 400) {
          const slice = expiredDocs.slice(i, i + 400);
          const batch = seventhFirestore.batch();
          slice.forEach((d) => batch.delete(d.ref));
          await batch.commit();
          deletedMessages += slice.length;
        }
      }

      if (remainingCount === 0) {
        await roomDocRef.delete().catch(() => {});
        activeOfflineRoomIds.delete(roomId);
        deletedRooms += 1;
      }
    }
  } catch (err) {
    console.error("❌ [7thDB 24h Sweeper] Error:", err.message);
  }

  if (deletedMessages > 0 || deletedRooms > 0) {
    console.log(
      `🧹 [7thDB 24h Sweeper] Purged ${deletedMessages} expired message(s) and ${deletedRooms} empty room doc(s).`
    );
  }
  return { deletedMessages, deletedRooms, timestamp: now };
}

setTimeout(() => sweepExpiredFirestoreMessages().catch(() => {}), 15000);
setInterval(
  () => sweepExpiredFirestoreMessages().catch(() => {}),
  SWEEP_INTERVAL_MS
);

// ==============================================================
// 🧠 IN-MEMORY LIVE STATE (ZERO FIRESTORE FOR LIVE SIGNALING)
// ==============================================================
const onlineUsers = new Map(); // uid -> user record
const emailToUid = new Map(); // emailLower -> uid
const socketToUid = new Map(); // socketId -> uid
const rooms = new Map(); // roomId -> { members: Set<uid>, activeThemeId, memberStates: Map }
const disconnectTimers = new Map(); // uid -> timeoutHandle

function cancelPendingDisconnect(uid) {
  const t = disconnectTimers.get(uid);
  if (t) {
    clearTimeout(t);
    disconnectTimers.delete(uid);
  }
}

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

function getWatchersForUser(myUid, myEmailLower) {
  const watchers = [];
  for (const [uid, u] of onlineUsers.entries()) {
    if (uid === myUid || !u.currentRoomId) continue;
    const matchesUid = u.targetUid && u.targetUid === myUid;
    const matchesEmail =
      myEmailLower && u.targetEmail && u.targetEmail === myEmailLower;
    if (matchesUid || matchesEmail) {
      watchers.push({
        watcherUid: u.uid,
        email: u.email,
        name: u.name,
        image: u.image,
        roomId: u.currentRoomId,
        isVip: Boolean(u.isVip),
        updatedAt: u.lastPing || Date.now(),
      });
    }
  }
  return watchers;
}

function handleUserLeavingRoom(uid, roomId, reason = "back_button") {
  cancelPendingDisconnect(uid);
  const user = onlineUsers.get(uid);
  const targetRoomId = roomId || user?.currentRoomId;
  if (!targetRoomId) return;

  const room = rooms.get(targetRoomId);
  const partner = getPartnerInRoom(targetRoomId, uid);
  const targetUid = user?.targetUid || partner?.uid || null;
  const targetEmail = user?.targetEmail || partner?.email || null;

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
    user.targetEmail = null;
    user.inCall = false;
  }

  const watchPayload = {
    roomId: targetRoomId,
    watcherUid: uid,
    targetUid,
    targetEmail,
    isWatching: false,
    reason,
    timestamp: Date.now(),
  };

  io.to(targetRoomId).emit("friend_watch_status", watchPayload);
  if (targetUid || targetEmail) {
    emitToUser(targetUid, targetEmail, "friend_watch_status", watchPayload);
    emitToUser(targetUid, targetEmail, "friend_partner_left_chat", {
      roomId: targetRoomId,
      droppedUid: uid,
      reason,
      timestamp: Date.now(),
    });
  }
}

// ==============================================================
// 💓 SMART STALE-CONNECTION CLEANUP (NEVER DROPS LIVE CONNECTED SOCKETS OR CALLS)
// ==============================================================
setInterval(() => {
  const now = Date.now();
  for (const [uid, user] of onlineUsers.entries()) {
    if (!user.currentRoomId) continue;
    if (user.inCall) continue; // Never heartbeat-drop an active call
    if (user.protectedUntil && now < user.protectedUntil) continue;

    // If Socket.IO itself is still connected, keep user alive!
    const liveSock = user.socketId ? io.sockets.sockets.get(user.socketId) : null;
    if (liveSock && liveSock.connected) {
      user.lastPing = now;
      continue;
    }

    if (now - (user.lastPing || 0) > HEARTBEAT_DROP_MS) {
      handleUserLeavingRoom(uid, user.currentRoomId, "timeout");
      onlineUsers.delete(uid);
    }
  }
}, 5000);

// ==============================================================
// 🌐 HTTP HEALTH & MANUAL SWEEP ENDPOINTS
// ==============================================================
app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "FriendListChatserver",
    onlineUsers: onlineUsers.size,
    activeRooms: rooms.size,
    trackedOfflineRooms: activeOfflineRoomIds.size,
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
  // 1. Register user (cancels any reconnect grace timer & immediately pushes current Live Watchers)
  socket.on("friend_register_user", (data = {}, ack) => {
    const { uid, email, name, image, isVip } = data;
    if (!uid) {
      ack?.({ ok: false, error: "uid required" });
      return;
    }

    cancelPendingDisconnect(uid);

    const emailLower = email ? String(email).toLowerCase() : null;
    const prev = onlineUsers.get(uid);

    if (prev && prev.socketId && prev.socketId !== socket.id) {
      socketToUid.delete(prev.socketId);
    }

    const userRecord = {
      socketId: socket.id,
      uid,
      email: emailLower || prev?.email || null,
      name: name || prev?.name || "User",
      image: image || prev?.image || null,
      isVip: Boolean(isVip ?? prev?.isVip),
      currentRoomId: prev?.currentRoomId || null,
      targetUid: prev?.targetUid || null,
      targetEmail: prev?.targetEmail || null,
      inCall: Boolean(prev?.inCall),
      protectedUntil: prev?.protectedUntil || 0,
      lastPing: Date.now(),
    };

    onlineUsers.set(uid, userRecord);
    socketToUid.set(socket.id, uid);
    if (userRecord.email) emailToUid.set(userRecord.email, uid);

    if (userRecord.currentRoomId) {
      socket.join(userRecord.currentRoomId);
    }

    // Immediately push current live watchers list so FriendListScreen updates in 0ms
    const watchers = getWatchersForUser(uid, userRecord.email);
    socket.emit("friend_watch_list", { targetUid: uid, watchers });

    ack?.({ ok: true, onlineCount: onlineUsers.size, watchers });
  });

  // 2. Query who is currently waiting in chat rooms with me ("Live 👀")
  socket.on("friend_get_live_watchers", (data = {}, ack) => {
    const myUid = data.uid || socketToUid.get(socket.id);
    const me = myUid ? onlineUsers.get(myUid) : null;
    const myEmailLower =
      me?.email || (data.email ? String(data.email).toLowerCase() : null);
    if (!myUid) {
      ack?.({ watchers: [] });
      return;
    }
    const watchers = getWatchersForUser(myUid, myEmailLower);
    ack?.({ watchers });
  });

  // 3. Join Friend Chat Room + Publish Live Watcher Presence Instantly
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

    cancelPendingDisconnect(uid);

    const emailLower = email ? String(email).toLowerCase() : null;
    const targetEmailLower = targetEmail
      ? String(targetEmail).toLowerCase()
      : null;
    const resolvedTargetUser = getUserByUidOrEmail(targetUid, targetEmailLower);
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
      targetEmail: targetEmailLower || resolvedTargetUser?.email || null,
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
      targetEmail: userRecord.targetEmail,
      name: userRecord.name,
      email: userRecord.email,
      image: userRecord.image,
      isVip: userRecord.isVip,
      isWatching: true,
      timestamp: Date.now(),
    };

    // Notify partner inside the room AND on My Friends screen
    socket.to(roomId).emit("friend_watch_status", watchPayload);
    if (resolvedTargetUid || userRecord.targetEmail) {
      emitToUser(
        resolvedTargetUid,
        userRecord.targetEmail,
        "friend_watch_status",
        watchPayload
      );
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
        targetEmail: userRecord.email,
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

  // 4. Explicit Leave Room (Back Button / Unmount)
  socket.on("friend_user_left_chat", (data = {}) => {
    const uid =
      data.watcherUid || data.senderUid || socketToUid.get(socket.id);
    if (!uid) return;
    const roomId = data.roomId || onlineUsers.get(uid)?.currentRoomId;
    if (roomId) socket.leave(roomId);
    handleUserLeavingRoom(uid, roomId, data.reason || "back_button");
  });

  // 5. Heartbeat Ping
  socket.on("friend_ping", (data = {}) => {
    const uid = data.uid || socketToUid.get(socket.id);
    if (!uid) return;
    cancelPendingDisconnect(uid);
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
    const durationMs = Number(data.durationMs) || 35000;
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

  // 8. Instant Block / Unblock Sync Across Both Devices
  socket.on("friend_block_status", (data = {}) => {
    const { roomId, targetUid, targetEmail, blocked } = data;
    const senderUid = data.senderUid || socketToUid.get(socket.id);
    const packet = {
      roomId,
      senderUid,
      senderEmail: data.senderEmail,
      blocked: Boolean(blocked),
      timestamp: Date.now(),
    };
    if (roomId) socket.to(roomId).emit("friend_block_status", packet);
    if (targetUid || targetEmail) {
      emitToUser(targetUid, targetEmail, "friend_block_status", packet);
    }
  });

  // ==============================================================
  // 📡 CHAT & CALL WEBRTC SIGNALING + SEAMLESS ICE RESTART
  // ==============================================================
  socket.on("friend_chat_webrtc_offer", (data = {}) => {
    if (!data?.roomId || !data?.offer) return;
    socket.to(data.roomId).emit("friend_chat_webrtc_offer", {
      ...data,
      senderUid: data.senderUid || socketToUid.get(socket.id),
    });
  });

  socket.on("friend_chat_webrtc_answer", (data = {}) => {
    if (!data?.roomId || !data?.answer) return;
    socket.to(data.roomId).emit("friend_chat_webrtc_answer", {
      ...data,
      senderUid: data.senderUid || socketToUid.get(socket.id),
    });
  });

  socket.on("friend_chat_webrtc_ice", (data = {}) => {
    if (!data?.roomId || !data?.candidate) return;
    socket.to(data.roomId).emit("friend_chat_webrtc_ice", {
      ...data,
      senderUid: data.senderUid || socketToUid.get(socket.id),
    });
  });

  socket.on("friend_chat_ice_restart", (data = {}) => {
    if (!data?.roomId) return;
    socket.to(data.roomId).emit("friend_chat_ice_restart", {
      ...data,
      senderUid: data.senderUid || socketToUid.get(socket.id),
    });
  });

  socket.on("friend_call_signal", (data = {}) => {
    const {
      roomId,
      targetUid,
      targetEmail,
      signalType,
      payload,
      senderUid,
      senderEmail,
    } = data;
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
      room.memberStates.set(uid, {
        ...prevState,
        ...data,
        updatedAt: Date.now(),
      });
      if (data.themeId) room.activeThemeId = data.themeId;
    }

    socket.to(roomId).emit("friend_call_media_sync", {
      ...data,
      senderUid: uid,
      timestamp: Date.now(),
    });
  });

  // ==============================================================
  // 💬 HYBRID MESSAGE ROUTER (ROOM-AWARE: INSTANT SOCKET WHEN IN ROOM)
  // ==============================================================
  socket.on("friend_hybrid_message", (data = {}, ack) => {
    const {
      roomId,
      targetUid,
      targetEmail,
      eventType = "MESSAGE",
      payload,
    } = data;
    const senderUid = data.senderUid || socketToUid.get(socket.id);

    if (!roomId) {
      ack?.({ ok: false, deliveredVia: "offline", inRoom: false });
      return;
    }

    const room = rooms.get(roomId);
    const targetUser = getUserByUidOrEmail(targetUid, targetEmail);

    if (eventType === "OFFLINE_TRACK_ONLY") {
      activeOfflineRoomIds.add(roomId);
      if (targetUser && targetUser.socketId && targetUser.currentRoomId !== roomId) {
        io.to(targetUser.socketId).emit("friend_unread_notification", {
          roomId,
          fromUid: senderUid,
          fromEmail: data.senderEmail,
          timestamp: Date.now(),
        });
      }
      ack?.({ ok: true, tracked: true });
      return;
    }

    // Check by actual room membership (works even if targetUid hasn't resolved yet!)
    const partnerInRoom = getPartnerInRoom(roomId, senderUid);
    const isPartnerPresent = Boolean(
      partnerInRoom ||
        (targetUser &&
          targetUser.currentRoomId === roomId &&
          room?.members.has(targetUser.uid))
    );

    if (isPartnerPresent) {
      socket.to(roomId).emit("friend_hybrid_message", {
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
    if (
      !seventhFirestore ||
      !roomId ||
      !Array.isArray(messageIds) ||
      messageIds.length === 0
    ) {
      return;
    }
    try {
      const roomDocRef = seventhFirestore
        .collection("FriendListChat")
        .doc(roomId);
      const colRef = roomDocRef.collection("messages");

      const batch = seventhFirestore.batch();
      messageIds.slice(0, 400).forEach((id) => {
        if (id) batch.delete(colRef.doc(String(id)));
      });
      await batch.commit();

      const remainingSnap = await colRef.limit(1).get();
      if (remainingSnap.empty) {
        await roomDocRef.delete().catch(() => {});
        activeOfflineRoomIds.delete(roomId);
      }
    } catch (e) {}
  });

  // ==============================================================
  // 🔌 GRACEFUL DISCONNECT (10s–18s RECONNECT WINDOW SO CALLS/CHATS NEVER FALSE-DROP)
  // ==============================================================
  socket.on("disconnect", () => {
    const uid = socketToUid.get(socket.id);
    socketToUid.delete(socket.id);
    if (!uid) return;

    const user = onlineUsers.get(uid);
    if (!user || user.socketId !== socket.id) return;

    const now = Date.now();
    const baseGrace = user.inCall
      ? CALL_DISCONNECT_GRACE_MS
      : CHAT_DISCONNECT_GRACE_MS;
    const protectedGrace =
      user.protectedUntil && user.protectedUntil > now
        ? Math.min(35000, user.protectedUntil - now + 2000)
        : 0;
    const waitMs = Math.max(baseGrace, protectedGrace);

    cancelPendingDisconnect(uid);
    const timer = setTimeout(() => {
      disconnectTimers.delete(uid);
      const latest = onlineUsers.get(uid);
      // Only remove if they did NOT reconnect with a new socket during the grace window
      if (latest && latest.socketId === socket.id) {
        if (latest.currentRoomId) {
          handleUserLeavingRoom(uid, latest.currentRoomId, "disconnected");
        }
        onlineUsers.delete(uid);
      }
    }, waitMs);

    disconnectTimers.set(uid, timer);
  });
});

server.listen(PORT, () => {
  console.log(`🚀 FriendListChatserver running on port ${PORT}`);
});