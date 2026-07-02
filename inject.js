/**
 * 千牛 JS 注入脚本 — 复刻 openbot inject.js，适配 macOS 千牛
 *
 * 功能：
 * 1. 建立 WebSocket 连接到本地 Python 服务器 (ws://127.0.0.1:41010)
 * 2. Hook 千牛事件系统，转发消息到 Python 后端
 * 3. 支持远程 eval 执行
 */

(function() {
  'use strict';

  // 防止重复注入
  if (typeof window.___openbot_mac_injected !== 'undefined') {
    console.log('[OpenBot] 已注入，跳过');
    return;
  }
  window.___openbot_mac_injected = true;

  const WS_URL = 'ws://127.0.0.1:41010';
  const HEARTBEAT_INTERVAL = 3000;  // 3 秒心跳
  const RECONNECT_DELAY = 3000;     // 3 秒重连
  const DEBUG_PROBES = true;
  const OPENBOT_INJECT_VERSION = 'coldstart-ingest-2026-07-02-2205';

  // 买家缓存 — 复刻 openbot _buyerCache
  window._buyerCache = window._buyerCache || new Map();

  // ─── WebSocket 连接管理 ───

  let heartbeatTimer = null;
  let reconnectTimer = null;

  function setupWebSocket() {
    if (window.chatWebsocket && window.chatWebsocket.readyState === WebSocket.OPEN) {
      return; // 已有活跃连接
    }
    if (window.chatWebsocket) {
      try {
        window.chatWebsocket.onclose = null;
        window.chatWebsocket.onerror = null;
        window.chatWebsocket.close();
      } catch (e) {}
      window.chatWebsocket = null;
    }

    let socket;
    try {
      socket = new WebSocket(WS_URL);
    } catch (e) {
      console.error('[OpenBot] WebSocket 创建失败:', e);
      scheduleReconnect();
      return;
    }

    socket.onopen = function() {
      if (window.chatWebsocket && window.chatWebsocket !== socket) {
        try { window.chatWebsocket.close(); } catch (e) {}
      }
      console.log('[OpenBot] WebSocket 已连接');
      window.chatWebsocket = socket;
      startHeartbeat();
      // 连接成功后清除重连定时器
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
    };

    socket.onmessage = async function(event) {
      try {
        let param = JSON.parse(event.data);
        if (param.method === 'executeNoWait') {
          socket.send(JSON.stringify({
            type: 'executeNoWaitAck',
            response: JSON.stringify({ ok: true, noWait: true })
          }));
          setTimeout(function() {
            try {
              eval(param.expression);
            } catch (err) {
              console.error('[OpenBot] EvalNoWait 错误:', err);
            }
          }, 0);
        } else if (param.method === 'execute') {
          // 远程 eval — 复刻 openbot 的 execute 模式
          try {
            const res = await eval(param.expression);
            socket.send(JSON.stringify({
              type: 'execute',
              response: JSON.stringify(res)
            }));
          } catch (err) {
            console.error('[OpenBot] Eval 错误:', err);
            socket.send(JSON.stringify({
              type: 'execute',
              response: JSON.stringify({ error: err.message })
            }));
          }
        }
      } catch (e) {
        console.error('[OpenBot] 消息处理错误:', e);
      }
    };

    socket.onclose = function() {
      console.log('[OpenBot] WebSocket 连接断开，准备重连...');
      if (window.chatWebsocket === socket) {
        window.chatWebsocket = null;
      }
      stopHeartbeat();
      scheduleReconnect();
    };

    socket.onerror = function(error) {
      console.error('[OpenBot] WebSocket 错误:', error);
    };
  }

  function startHeartbeat() {
    stopHeartbeat();
    heartbeatTimer = setInterval(function() {
      if (window.chatWebsocket && window.chatWebsocket.readyState === WebSocket.OPEN) {
        try {
          window.chatWebsocket.send(JSON.stringify({ type: 'hi' }));
        } catch (e) {
          try { window.chatWebsocket.close(); } catch (closeErr) {}
          window.chatWebsocket = null;
          stopHeartbeat();
          scheduleReconnect();
        }
      } else {
        stopHeartbeat();
        if (window.chatWebsocket) {
          try { window.chatWebsocket.close(); } catch (e) {}
          window.chatWebsocket = null;
        }
        scheduleReconnect();
      }
    }, HEARTBEAT_INTERVAL);
  }

  function stopHeartbeat() {
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
  }

  function scheduleReconnect() {
    if (reconnectTimer) return;
    reconnectTimer = setTimeout(function() {
      reconnectTimer = null;
      setupWebSocket();
    }, RECONNECT_DELAY);
  }

  // ─── 消息发送工具 ───

  function sendMessage(type, data) {
    if (window.chatWebsocket && window.chatWebsocket.readyState === WebSocket.OPEN) {
      window.chatWebsocket.send(JSON.stringify({
        type: type,
        response: typeof data === 'string' ? data : JSON.stringify(data)
      }));
    }
  }

  function withTimeout(promise, timeoutMs, label) {
    return Promise.race([
      Promise.resolve(promise),
      new Promise(function(_, reject) {
        setTimeout(function() {
          reject(new Error((label || 'promise') + ' timeout after ' + timeoutMs + 'ms'));
        }, timeoutMs);
      })
    ]);
  }

  window.__openbotMacState = window.__openbotMacState || {
    buyerId: '',
    lastMessageSendTime: '',
    activeUser: null,
    activeUsers: {},
    nativeProbeStartedFor: {},
    openChatRequestedFor: {},
    activeUserRefreshStarted: false,
    pageMessageSeen: {}
  };

  // ─── 千牛事件 Hook — 复刻 openbot 的事件拦截 ───

  // 1. Hook onEventNotify — 复刻 inject.js 第 62-82 行。
  // macOS 部分 WebView 会在页面脚本加载后才注入 onEventNotify，所以需要持续尝试包裹。
  function wrapOnEventNotify() {
    if (typeof window.onEventNotify !== 'function' ||
        window.onEventNotify.__openbotWrapped) {
      return;
    }

    var originalOnEventNotify = window.onEventNotify;
    var wrappedOnEventNotify = function(sid, name, a, data) {
      sendMessage('rawOnEventNotify', {
        sid: sid,
        name: name,
        status: a,
        data: data,
        href: String(location.href)
      });

      // 调用原始处理函数
      originalOnEventNotify.apply(this, arguments);

      if (typeof name === 'string' && name.indexOf('im.singlemsg.') === 0) {
        sendMessage('macOnEventNotify', {
          sid: sid,
          name: name,
          status: a,
          data: data,
          href: String(location.href)
        });
        if (name === 'im.singlemsg.onReceiveNewMsg') {
          sendMessage('macReceiveNewMsgNotify', {
            sid: sid,
            name: name,
            status: a,
            data: data,
            href: String(location.href)
          });
        } else if (name === 'im.singlemsg.onShopRobotReceriveNewMsgs') {
          sendMessage('macShopRobotNewMsgs', {
            sid: sid,
            name: name,
            status: a,
            data: data,
            href: String(location.href)
          });
        }
        console.log('[OpenBot] onEventNotify:', name, data);
        return;
      }

      try {
        name = JSON.parse(name);
      } catch (e) {
        return;
      }

      const loginID = window._vs ? window._vs.loginID : {};

      if (sid.indexOf('onConversationChange') >= 0) {
        updateFromConversation(name);
        sendMessage('onConversationChange', { loginID: loginID, conversation: name });
        console.log('[OpenBot] onConversationChange:', name.nick);
      } else if (sid.indexOf('onConversationAdd') >= 0) {
        updateFromConversation(name);
        sendMessage('onConversationAdd', { loginID: loginID, conversation: name });
      } else if (sid.indexOf('onConversationClose') >= 0) {
        updateFromConversation(name);
        sendMessage('onConversationClose', { loginID: loginID, conversation: name });
      } else if (sid.indexOf('OnChatDlgActive') >= 0) {
        sendMessage('onChatDlgActive', {
          loginID: loginID,
          conversation: window._vs ? window._vs.conversationID : ''
        });
      }
    };
    wrappedOnEventNotify.__openbotWrapped = true;
    wrappedOnEventNotify.__openbotOriginal = originalOnEventNotify;
    window.onEventNotify = wrappedOnEventNotify;
  }
  wrapOnEventNotify();
  setInterval(wrapOnEventNotify, 1000);

  try {
    var currentOnEventNotify = window.onEventNotify;
    Object.defineProperty(window, 'onEventNotify', {
      configurable: true,
      get: function() {
        return currentOnEventNotify;
      },
      set: function(fn) {
        currentOnEventNotify = fn;
        setTimeout(function() {
          wrapOnEventNotify();
        }, 0);
      }
    });
    if (currentOnEventNotify) {
      window.onEventNotify = currentOnEventNotify;
    }
  } catch (e) {
    // Some native-injected globals may be non-configurable; interval wrapping above is the fallback.
  }

  // 2. 千牛消息中心通知 — 复刻 inject.js 第 85-93 行
  if (typeof QN !== 'undefined' && typeof QN.regEvent === 'function') {
    try {
      QN.regEvent('bench.msgcenter.newmsgnotify', function(res) {
        sendMessage('messageCenterNotify', res);
      });
    } catch (e) {
      console.warn('[OpenBot] QN.regEvent 注册失败:', e);
    }
  }

  // 3. IM SDK 新消息监听 — 复刻 inject.js 第 97-127 行
  if (typeof imsdk !== 'undefined' && typeof window.onInvokeNotifyDelegate === 'undefined') {
    try {
      imsdk.on(['im.singlemsg.onReceiveNewMsg'], function(cids) {
        cids.forEach(async function(cid) {
          var ccode = cid && cid.ccode;
          if (ccode) {
            var remoteMessages = await getRemoteMessages(ccode);
            if (remoteMessages && remoteMessages.result && remoteMessages.result.length) {
              sendMessage('receiveNewMsg', remoteMessages);
            } else {
              schedulePageMessageScans('remoteEmpty:' + ccode);
            }
          }
          var conv = getCacheConv(cid.ccode);
          if (conv == undefined) {
            conv = await getRemoteMsg(cid.ccode);
          }
          sendMessage('onShopRobotReceriveNewMsgs', {
            loginID: window._vs ? window._vs.loginID : {},
            conversation: conv
          });
          console.log('[OpenBot] onShopRobotReceriveNewMsgs:', JSON.stringify(conv));
        });
      });

      // Hook onInvokeNotify — 复刻 inject.js 第 118-127 行
      window.onInvokeNotifyDelegate = window.onInvokeNotify;
      window.onInvokeNotify = function(sid, status, response) {
        window.onInvokeNotifyDelegate(sid, status, response);

        try {
          var task = typeof TASK_CACHE !== 'undefined' ? TASK_CACHE[sid] : null;
          if (task && task.config &&
              task.config.fn === 'im.singlemsg.GetNewMsg' &&
              task.config.param &&
              task.config.param.ccode === window._conversationId.ccode) {
            sendMessage('receiveNewMsg', response);
          }
        } catch (e) {
          console.error('[OpenBot] onInvokeNotify 处理错误:', e);
        }
      };
    } catch (e) {
      console.warn('[OpenBot] imsdk 监听注册失败:', e);
    }
  }

  // 4. macOS 新版聊天侧栏/插件事件。9.x 千牛会把聊天摘要和插件通知投到
  // onAbilityEventNotify / onAbilityPrivateEventNotify，而不是旧版 imsdk 页面。
  function wrapAbilityNotify(name) {
    const original = window[name];
    if (typeof original === 'function' && !original.__openbotWrapped) {
      const wrapped = function(sid, eventName, status, data) {
        try {
          if (eventName === 'workbench.servicesummary.pull') {
            try {
              var parsed = typeof data === 'string' ? JSON.parse(data) : data;
              if (parsed && parsed.buyerId) {
                window.__openbotMacState.buyerId = String(parsed.buyerId);
                window.__openbotMacState.lastMessageSendTime = String(parsed.lastMessageSendTime || '');
                requestActiveUserRefresh();
                scheduleOpenChatForBuyer(parsed.buyerId);
              }
            } catch (parseErr) {
              console.error('[OpenBot] servicesummary.pull 解析失败:', parseErr);
            }
          } else if (eventName === 'wangwang.recvU2UMsgBatch' ||
              eventName === 'wangwang.active_contact_changed') {
            handleNativeWangwangForOpenChat(eventName, data);
            sendMessage('nativeWangwangEvent', {
              sid: sid,
              name: eventName,
              status: status,
              data: data,
              href: String(location.href)
            });
          }
          sendMessage(name, {
            sid: sid,
            name: eventName,
            status: status,
            data: data,
            href: String(location.href)
          });
          console.log('[OpenBot] ' + name + ':', eventName, data);
        } catch (e) {
          console.error('[OpenBot] ' + name + ' 处理错误:', e);
        }
        return original.apply(this, arguments);
      };
      wrapped.__openbotWrapped = true;
      window[name] = wrapped;
    } else if (typeof original !== 'function') {
      window[name] = function(sid, eventName, status, data) {
        if (eventName === 'workbench.servicesummary.pull') {
          try {
            var parsed = typeof data === 'string' ? JSON.parse(data) : data;
            if (parsed && parsed.buyerId) {
              window.__openbotMacState.buyerId = String(parsed.buyerId);
              window.__openbotMacState.lastMessageSendTime = String(parsed.lastMessageSendTime || '');
              requestActiveUserRefresh();
              scheduleOpenChatForBuyer(parsed.buyerId);
            }
          } catch (parseErr) {
            console.error('[OpenBot] servicesummary.pull 解析失败:', parseErr);
          }
        } else if (eventName === 'wangwang.recvU2UMsgBatch' ||
            eventName === 'wangwang.active_contact_changed') {
          handleNativeWangwangForOpenChat(eventName, data);
          sendMessage('nativeWangwangEvent', {
            sid: sid,
            name: eventName,
            status: status,
            data: data,
            href: String(location.href)
          });
        }
        sendMessage(name, {
          sid: sid,
          name: eventName,
          status: status,
          data: data,
          href: String(location.href)
        });
        console.log('[OpenBot] ' + name + ':', eventName, data);
      };
      window[name].__openbotWrapped = true;
    }
  }

  wrapAbilityNotify('onAbilityEventNotify');
  wrapAbilityNotify('onAbilityPrivateEventNotify');

  function updateActiveUserFromNativeResult(resultText) {
    try {
      if (!resultText) return;
      var data = typeof resultText === 'string' ? JSON.parse(resultText) : resultText;
      if (!data || !data.cid || !data.uid) return;
      window.__openbotMacState.activeUser = data;
      window.__openbotMacState.activeUsers[data.cid] = data;
      if (data.securityUID) {
        window.__openbotMacState.buyerId = String(data.securityUID);
      }
      reportProbe('activeUser:detected', data);
      scheduleOpenChatForActiveUser(data);
    } catch (e) {
      // Not every native callback carries active-user data.
    }
  }

  function isOpenBotBridgePage() {
    var href = String(location.href || '');
    return href.indexOf('openbot-bridge') >= 0 ||
      href.indexOf('qn-cs-chat-top-summary') >= 0;
  }

  function isNativeChatEventPage() {
    var href = String(location.href || '');
    return /alires-webui\/dx-h5\/index\.html/i.test(href) ||
      /alires-webui\/Message\//i.test(href);
  }

  function canUseWorkbenchInvoke() {
    return typeof workbench !== 'undefined' &&
      workbench.application &&
      typeof workbench.application.invoke === 'function';
  }

  function requestActiveUserRefresh() {
    if (!isOpenBotBridgePage() || !canUseWorkbenchInvoke()) return;
    if (window.__openbotMacState.activeUserRefreshStarted) return;
    window.__openbotMacState.activeUserRefreshStarted = true;

    setTimeout(function() {
      try {
        var invoke = workbench.application.invoke.bind(workbench.application);
        invoke('getActiveUser', {}, function(res) {
          updateActiveUserFromNativeResult(res);
        });
        setTimeout(function() {
          try {
            invoke({ event: 'getActiveUser', param: {} }, function(res) {
              updateActiveUserFromNativeResult(res);
            });
          } catch (e) {}
        }, 600);
      } catch (e) {
        reportProbe('activeUser:refreshError', String(e && e.message || e));
      } finally {
        setTimeout(function() {
          window.__openbotMacState.activeUserRefreshStarted = false;
        }, 2500);
      }
    }, 100);
  }

  function buildOpenChatParams(activeUser, buyerId) {
    activeUser = activeUser || {};
    buyerId = String(buyerId || activeUser.securityUID || '');
    var uid = activeUser.uid || (buyerId ? 'cntaobao' + buyerId : '');
    var nick = activeUser.user_nick || activeUser.dnick || uid;
    return {
      uid: uid,
      nick: nick,
      cid: activeUser.cid || '',
      securityUID: activeUser.securityUID || buyerId,
      targetId: buyerId || activeUser.securityUID || '',
      bizDomain: activeUser.bizDomain || 'taobao',
      bizType: activeUser.bizType || '11001'
    };
  }

  function parseNativeWangwangData(data) {
    try {
      var parsed = typeof data === 'string' ? JSON.parse(data) : data;
      if (Array.isArray(parsed)) return parsed;
      if (parsed && Array.isArray(parsed.data)) return parsed.data;
    } catch (e) {}
    return [];
  }

  function userFromWangwangEvent(item) {
    if (!item) return null;
    var securityUID = String(item.securityUID || '');
    var fromuid = String(item.fromuid || '');
    var nick = String(item.nick || '');
    if (!securityUID || !fromuid) return null;
    if (securityUID === '2219383781151' || nick === '山西携旅旅游专营店') return null;
    return {
      uid: fromuid,
      user_nick: fromuid,
      dnick: nick,
      securityUID: securityUID,
      bizDomain: 'taobao',
      bizType: '11001',
      cid: ''
    };
  }

  function handleNativeWangwangForOpenChat(eventName, data) {
    if (eventName !== 'wangwang.recvU2UMsgBatch' &&
        eventName !== 'wangwang.active_contact_changed') {
      return;
    }
    parseNativeWangwangData(data).forEach(function(item) {
      var user = userFromWangwangEvent(item);
      if (!user) return;
      window.__openbotMacState.buyerId = user.securityUID;
      if (!window.__openbotMacState.activeUser ||
          window.__openbotMacState.activeUser.securityUID !== user.securityUID) {
        window.__openbotMacState.activeUser = user;
      }
      reportProbe('wangwangUser:detected', {
        uid: user.uid,
        nick: user.dnick,
        securityUID: user.securityUID,
        type: item.type,
        hasMessage: !!item.message
      });
      scheduleOpenChatForActiveUser(user, user.securityUID);
    });
  }

  function scheduleOpenChatForBuyer(buyerId) {
    buyerId = String(buyerId || '');
    if (!buyerId) return;
    scheduleOpenChatForActiveUser(window.__openbotMacState.activeUser, buyerId);
  }

  function scheduleOpenChatForActiveUser(activeUser, fallbackBuyerId) {
    if (!isOpenBotBridgePage() || !canUseWorkbenchInvoke()) return;
    var buyerId = String(fallbackBuyerId || (activeUser && activeUser.securityUID) || window.__openbotMacState.buyerId || '');
    var key = (activeUser && activeUser.cid) || buyerId;
    if (!key || window.__openbotMacState.openChatRequestedFor[key]) return;
    window.__openbotMacState.openChatRequestedFor[key] = true;

    setTimeout(function() {
      var params = buildOpenChatParams(activeUser, buyerId);
      var invoke = workbench.application.invoke.bind(workbench.application);
      reportProbe('openChat:autoStart', {
        key: key,
        href: String(location.href),
        params: {
          uid: params.uid,
          nick: params.nick,
          cid: params.cid,
          securityUID: params.securityUID,
          targetId: params.targetId,
          bizDomain: params.bizDomain,
          bizType: params.bizType
        }
      });

      var attempts = [];
      if (params.cid) {
        attempts.push(['cid', { cid: params.cid, bizDomain: params.bizDomain }]);
      }
      if (params.targetId) {
        attempts.push(['targetId', { targetId: params.targetId, bizDomain: params.bizDomain }]);
      }
      if (params.uid) {
        attempts.push(['activeUser', params]);
      }

      attempts.slice(0, 3).forEach(function(attempt, index) {
        setTimeout(function() {
          try {
            var done = false;
            var ret = invoke('qn.openChat', attempt[1], function(res) {
              done = true;
              reportProbe('openChat:autoCallback', { mode: attempt[0], result: res });
            });
            setTimeout(function() {
              if (!done) reportProbe('openChat:autoReturn', { mode: attempt[0], result: ret });
            }, 500);
          } catch (e) {
            reportProbe('openChat:autoError', {
              mode: attempt[0],
              error: String(e && e.message || e)
            });
          }
        }, index * 700);
      });
    }, 300);
  }

  function wrapRawNotify(name) {
    const original = window[name];
    if (typeof original === 'function' && !original.__openbotWrapped) {
      const wrapped = function() {
        try {
          if (arguments.length >= 3) {
            updateActiveUserFromNativeResult(arguments[2]);
          }
          sendMessage(name, {
            args: Array.prototype.slice.call(arguments).map(function(arg) {
              if (typeof arg === 'string') return arg;
              try { return JSON.stringify(arg); } catch (e) { return String(arg); }
            }),
            href: String(location.href)
          });
          console.log('[OpenBot] ' + name + ':', arguments);
        } catch (e) {
          console.error('[OpenBot] ' + name + ' 处理错误:', e);
        }
        return original.apply(this, arguments);
      };
      wrapped.__openbotWrapped = true;
      window[name] = wrapped;
    } else if (typeof original !== 'function') {
      window[name] = function() {
        if (arguments.length >= 3) {
          updateActiveUserFromNativeResult(arguments[2]);
        }
        sendMessage(name, {
          args: Array.prototype.slice.call(arguments).map(function(arg) {
            if (typeof arg === 'string') return arg;
            try { return JSON.stringify(arg); } catch (e) { return String(arg); }
          }),
          href: String(location.href)
        });
        console.log('[OpenBot] ' + name + ':', arguments);
      };
      window[name].__openbotWrapped = true;
    }
  }

  [
    'onInvokeNotify',
    'onAbilityInvokeNotify',
    'onAbilityNewInvokeNotify',
    'onAbilityPrivateInvokeNotify',
    'onAbilityPrivateNewNotify'
  ].forEach(wrapRawNotify);

  function safeKeys(obj, limit) {
    try {
      if (!obj) return [];
      return Object.keys(obj).slice(0, limit || 50);
    } catch (e) {
      return [];
    }
  }

  function buildOpenbotContextProbe(reason) {
    var hasImsdk = typeof imsdk !== 'undefined';
    var hasVs = typeof window._vs !== 'undefined';
    var hasDb = typeof window._db !== 'undefined';
    var taskCache = typeof TASK_CACHE !== 'undefined' ? TASK_CACHE : undefined;
    var conversationId = typeof window._conversationId !== 'undefined' ? window._conversationId : undefined;
    return {
      reason: reason,
      openbotInjectVersion: OPENBOT_INJECT_VERSION,
      href: String(location.href),
      title: String(document.title || ''),
      readyState: String(document.readyState || ''),
      hasImsdk: hasImsdk,
      hasImsdkInvoke: hasImsdk && typeof imsdk.invoke === 'function',
      hasImsdkOn: hasImsdk && typeof imsdk.on === 'function',
      hasVs: hasVs,
      vsKeys: safeKeys(window._vs, 40),
      loginID: hasVs ? window._vs.loginID : undefined,
      conversationID: hasVs ? window._vs.conversationID : undefined,
      hasDb: hasDb,
      dbKeys: safeKeys(window._db, 40),
      hasMsgDataMap: hasDb && !!window._db.msgDataMap,
      msgDataMapType: hasDb && window._db.msgDataMap ? Object.prototype.toString.call(window._db.msgDataMap) : '',
      hasTaskCache: typeof TASK_CACHE !== 'undefined',
      taskCacheKeys: safeKeys(taskCache, 20),
      hasConversationId: typeof window._conversationId !== 'undefined',
      conversationId: conversationId,
      hasOnEventNotify: typeof window.onEventNotify === 'function',
      hasOnInvokeNotify: typeof window.onInvokeNotify === 'function',
      hasQN: typeof QN !== 'undefined',
      hasWorkbench: typeof workbench !== 'undefined',
      interestingGlobals: Object.keys(window).filter(function(k) {
        return /^(QN|qn|workbench|ability|imsdk|_vs|_db|TASK|_conversation|onEvent|onInvoke)/i.test(k);
      }).slice(0, 120)
    };
  }

  function reportOpenbotContext(reason) {
    reportProbe('openbotContextProbe', buildOpenbotContextProbe(reason));
  }

  setTimeout(function() {
    var context = buildOpenbotContextProbe('bridgeReady');
    context.hasWorkbenchInvoke = typeof workbench !== 'undefined' &&
      workbench.application &&
      typeof workbench.application.invoke === 'function';
    context.hasQnSdk = typeof qnSdk !== 'undefined' || typeof QNPlugin !== 'undefined';
    context.hasAbilityNotify = typeof onAbilityEventNotify === 'function' ||
      typeof onAbilityPrivateEventNotify === 'function';
    context.globalKeys = context.interestingGlobals;
    sendMessage('bridgeReady', context);
    reportOpenbotContext('timer:1200');
    if (!isNativeChatEventPage()) {
      startPageMessageScanner();
    }
  }, 1200);
  setTimeout(function() { reportOpenbotContext('timer:4000'); }, 4000);
  setTimeout(function() { reportOpenbotContext('timer:10000'); }, 10000);
  setTimeout(function() { reportOpenbotContext('timer:20000'); }, 20000);

  function reportProbe(name, value) {
    try {
      sendMessage('workbenchProbe', {
        name: name,
        value: value,
        href: String(location.href)
      });
    } catch (e) {
      console.error('[OpenBot] workbenchProbe 上报失败:', e);
    }
  }

  function normalizeText(text) {
    return String(text || '')
      .replace(/\s+/g, ' ')
      .replace(/^\s+|\s+$/g, '');
  }

  function isLikelyBuyerText(text) {
    text = normalizeText(text);
    if (!text || text.length < 1 || text.length > 500) return false;
    if (/^(发送|转人工|关闭|取消|确定|复制|删除|更多|智能客服|千牛商家工作台|天猫商家中心|设置|操作指南)$/.test(text)) return false;
    if (/OpenBot连通测试|山西携旅旅游专营店|智能客服全新升级|助力客服高效接待|客服面板设置|店铺身份|店铺消费|优惠券|邀请关注|邀请入会|邀请入群|添加备注|好评\d|非会员|非粉丝|新客/.test(text)) return false;
    if (/^(https?:\/\/|data:|blob:)/i.test(text)) return false;
    if (/^[\d:：\-\s/年月日.]+$/.test(text)) return false;
    if (/^[A-Za-z0-9+/=]{18,}$/.test(text)) return false;
    return true;
  }

  function activeBuyerInfo() {
    var activeUser = window.__openbotMacState.activeUser || {};
    var href = String(location.href || '');
    var params = {};
    try {
      var query = href.split('?')[1] || '';
      query.split('&').forEach(function(part) {
        var pair = part.split('=');
        if (pair[0]) params[pair[0]] = decodeURIComponent(pair.slice(1).join('=') || '');
      });
    } catch (e) {}
    var uid = activeUser.uid || params.chatNick || '';
    var nick = activeUser.dnick || activeUser.user_nick || params.chatNick || uid;
    return {
      uid: uid,
      nick: nick,
      securityUID: activeUser.securityUID || params.securityUID || params.targetId || '',
      cid: activeUser.cid || ''
    };
  }

  function emitPageMessageCandidate(source, text, meta) {
    text = normalizeText(text);
    if (!isLikelyBuyerText(text)) return;
    var buyer = activeBuyerInfo();
    var key = source + ':' + (buyer.uid || buyer.nick || buyer.securityUID || '') + ':' + text;
    if (window.__openbotMacState.pageMessageSeen[key]) return;
    window.__openbotMacState.pageMessageSeen[key] = Date.now();
    sendMessage('pageMessageCandidate', {
      source: source,
      messageText: text,
      buyerNick: buyer.nick,
      buyerUid: buyer.securityUID || buyer.uid,
      buyer: buyer,
      meta: meta || {},
      href: String(location.href)
    });
  }

  function classText(el) {
    try {
      return String((el.className && el.className.baseVal) || el.className || '') + ' ' +
        String(el.id || '') + ' ' + String(el.getAttribute('role') || '');
    } catch (e) {
      return '';
    }
  }

  function isExcludedPanel(marker, text) {
    return /(notice|TopNotice|ModuleContainer|SettingGuide|CustomerTags|MemberName|MemberDesc|collapse|coupon|balloon|tooltip|setting|tags|member|profile|guide|notice-content|ws-notice|next-slick|next-balloon|next-box)/i.test(marker) ||
      /智能客服全新升级|操作指南|客服面板设置|店铺身份|店铺消费|优惠券|邀请关注|邀请入会|邀请入群|添加备注/.test(text);
  }

  function isMessageBubbleMarker(marker) {
    return /(message|msg|bubble|chat.*item|item.*chat|dialog.*item|conversation.*item|Message|Msg|Bubble|ChatItem|MsgItem|im-message|ww-message)/i.test(marker) &&
      !/(notice|TopNotice|ModuleContainer|SettingGuide|CustomerTags|MemberName|MemberDesc|collapse|coupon|balloon|tooltip|setting|tags|member|profile|guide|next-|ws-notice)/i.test(marker);
  }

  function collectDomMessageCandidates() {
    if (!/web_chat-packer\/recent|Intelligent-customer-service|qn-cs-chat-top-summary/i.test(String(location.href || ''))) return [];
    var candidates = [];
    var nodes = [];
    try {
      nodes = Array.prototype.slice.call(document.querySelectorAll('div,span,p,pre,textarea,[class],[data-spm],[role]')).slice(0, 3000);
    } catch (e) {
      return [];
    }
    nodes.forEach(function(el) {
      var text = normalizeText(el.innerText || el.textContent || el.value || '');
      if (!isLikelyBuyerText(text)) return;
      var marker = classText(el);
      if (isExcludedPanel(marker, text)) return;
      var messageBubble = isMessageBubbleMarker(marker);
      var rect = null;
      try {
        var r = el.getBoundingClientRect();
        if (r.width <= 0 || r.height <= 0) return;
        rect = { x: Math.round(r.x), y: Math.round(r.y), w: Math.round(r.width), h: Math.round(r.height) };
      } catch (e) {}
      if (!messageBubble && text.length > 80) return;
      candidates.push({
        source: messageBubble ? 'dom:messageBubble' : 'dom:text',
        text: text,
        tag: String(el.tagName || '').toLowerCase(),
        marker: marker.slice(0, 160),
        rect: rect
      });
    });
    var seen = {};
    return candidates.filter(function(item) {
      var key = item.source + ':' + item.text;
      if (seen[key]) return false;
      seen[key] = true;
      return true;
    }).slice(0, 30);
  }

  function collectStorageMessageCandidates() {
    var candidates = [];
    ['localStorage', 'sessionStorage'].forEach(function(storageName) {
      try {
        var storage = window[storageName];
        if (!storage) return;
        for (var i = 0; i < Math.min(storage.length, 300); i++) {
          var key = storage.key(i);
          var value = storage.getItem(key) || '';
          if (!/(msg|message|chat|conversation|wangwang|im|客服|会话|消息)/i.test(key + ' ' + value.slice(0, 200))) continue;
          extractTextsFromValue(value).slice(0, 10).forEach(function(text) {
            candidates.push({ source: storageName, key: key, text: text });
          });
        }
      } catch (e) {}
    });
    return candidates.slice(0, 30);
  }

  function extractTextsFromValue(value) {
    var texts = [];
    function visit(v, depth) {
      if (depth > 4 || texts.length > 30 || v == null) return;
      if (typeof v === 'string') {
        var text = normalizeText(v);
        if (isLikelyBuyerText(text)) texts.push(text);
        return;
      }
      if (Array.isArray(v)) {
        v.slice(0, 80).forEach(function(item) { visit(item, depth + 1); });
        return;
      }
      if (typeof v === 'object') {
        Object.keys(v).slice(0, 80).forEach(function(k) {
          if (/(text|content|message|msg|summary|title|nick|question|answer)/i.test(k)) {
            visit(v[k], depth + 1);
          } else if (depth < 2) {
            visit(v[k], depth + 1);
          }
        });
      }
    }
    try {
      visit(JSON.parse(value), 0);
    } catch (e) {
      value.split(/["'\n\r]/).forEach(function(part) {
        var text = normalizeText(part);
        if (isLikelyBuyerText(text)) texts.push(text);
      });
    }
    var seen = {};
    return texts.filter(function(text) {
      if (seen[text]) return false;
      seen[text] = true;
      return true;
    });
  }

  function scanPageMessages(reason) {
    var dom = collectDomMessageCandidates();
    var storage = collectStorageMessageCandidates();
    if (DEBUG_PROBES && (dom.length || storage.length)) {
      reportProbe('messageCandidates', {
        reason: reason,
        dom: dom.slice(0, 20),
        storage: storage.slice(0, 20)
      });
    }
    var emittedBubble = false;
    dom.forEach(function(item) {
      if (item.source === 'dom:messageBubble') {
        emittedBubble = true;
        emitPageMessageCandidate(item.source, item.text, {
          tag: item.tag,
          marker: item.marker,
          rect: item.rect,
          reason: reason
        });
      }
    });
    if (!emittedBubble && /conversationUnread|autoOpen|alreadyActive|remoteEmpty/i.test(String(reason || ''))) {
      dom.filter(function(item) {
        return item.source === 'dom:text';
      }).slice(-3).forEach(function(item) {
        emitPageMessageCandidate(item.source, item.text, {
          tag: item.tag,
          marker: item.marker,
          rect: item.rect,
          reason: reason,
          fallback: true
        });
      });
    }
  }

  function schedulePageMessageScans(reason) {
    [200, 800, 1600, 3000, 5000].forEach(function(delay) {
      setTimeout(function() {
        scanPageMessages((reason || 'scheduled') + ':' + delay);
      }, delay);
    });
  }

  function startPageMessageScanner() {
    if (window.__openbotMacState.pageMessageScannerStarted) return;
    window.__openbotMacState.pageMessageScannerStarted = true;
    [1500, 3000, 6000, 10000, 15000].forEach(function(delay) {
      setTimeout(function() { scanPageMessages('timer:' + delay); }, delay);
    });
    try {
      var timer = null;
      var observer = new MutationObserver(function() {
        clearTimeout(timer);
        timer = setTimeout(function() { scanPageMessages('mutation'); }, 500);
      });
      observer.observe(document.documentElement || document.body, {
        childList: true,
        subtree: true,
        characterData: true
      });
    } catch (e) {}
  }
  window.__openbotScanPageMessages = function(reason) {
    scanPageMessages(reason || 'manual');
  };

  function registerAbilityEvent(eventName) {
    try {
      if (!window.__openbotMacState.registeredEvents) {
        window.__openbotMacState.registeredEvents = {};
      }
      if (window.__openbotMacState.registeredEvents[eventName]) return;
      if (!window.abilitycenter || !window.abilitycenter.ability ||
          typeof window.abilitycenter.ability.invoke !== 'function') {
        return;
      }
      var sid = String(
        typeof window.abilitycenter.createSequenceId === 'function'
          ? window.abilitycenter.createSequenceId()
          : Date.now()
      );
      window.__openbotMacState.registeredEvents[eventName] = sid;
      window.abilitycenter.ability.invoke(
        sid,
        'regEvent',
        JSON.stringify({ eventName: eventName })
      );
      if (DEBUG_PROBES) {
        reportProbe('ability.regEvent', { eventName: eventName, sid: sid });
      }
    } catch (e) {
      reportProbe('ability.regEvent:error', {
        eventName: eventName,
        error: String(e && e.message || e)
      });
    }
  }

  function registerNativeEvents() {
    [
      'wangwang.recvU2UMsgBatch',
      'wangwang.active_contact_changed',
      'workbench.servicesummary.pull',
      'workbench.servicesummary.state'
    ].forEach(registerAbilityEvent);
  }

  setTimeout(registerNativeEvents, 1500);
  setInterval(registerNativeEvents, 15000);

  function describeObject(name, obj) {
    try {
      if (!obj) {
        reportProbe(name, { exists: false });
        return;
      }
      var keys = [];
      try { keys = Object.keys(obj).slice(0, 80); } catch (e) {}
      var props = {};
      keys.slice(0, 30).forEach(function(k) {
        try {
          var v = obj[k];
          props[k] = {
            type: typeof v,
            length: typeof v === 'function' ? v.length : undefined,
            source: typeof v === 'function' ? String(v).slice(0, 220) : undefined
          };
        } catch (e) {
          props[k] = { error: String(e && e.message || e) };
        }
      });
      reportProbe(name, {
        exists: true,
        type: typeof obj,
        keys: keys,
        props: props
      });
    } catch (e) {
      reportProbe(name + ':describeError', String(e && e.message || e));
    }
  }

  function callMaybePromise(name, fn) {
    try {
      var done = false;
      var ret = fn(function(res) {
        done = true;
        reportProbe(name + ':callback', res);
      });
      if (ret && typeof ret.then === 'function') {
        ret.then(function(res) {
          reportProbe(name + ':promise', res);
        }).catch(function(err) {
          reportProbe(name + ':promiseError', String(err && err.message || err));
        });
      } else {
        setTimeout(function() {
          if (!done) reportProbe(name + ':return', ret);
        }, 500);
      }
    } catch (e) {
      reportProbe(name + ':error', String(e && e.message || e));
    }
  }

  function runNativeChatProbe() {
    if (!DEBUG_PROBES) return;
    var buyerId = window.__openbotMacState.buyerId;
    var activeUser = window.__openbotMacState.activeUser || {};
    if (!buyerId && !activeUser.uid && !activeUser.cid) return;
    if (typeof workbench === 'undefined' ||
        !workbench.application ||
        typeof workbench.application.invoke !== 'function') {
      return;
    }
    var probeKey = activeUser.cid || activeUser.uid || buyerId;
    if (window.__openbotMacState.nativeProbeStartedFor[probeKey]) return;
    window.__openbotMacState.nativeProbeStartedFor[probeKey] = true;

    var invoke = workbench.application.invoke.bind(workbench.application);
    reportProbe('nativeChatProbe:start', {
      probeKey: probeKey,
      buyerId: buyerId,
      activeUser: activeUser,
      lastMessageSendTime: window.__openbotMacState.lastMessageSendTime
    });

    var emptyText = '';
    var draftText = 'OpenBot连通测试，请勿发送';
    var uid = activeUser.uid || ('cntaobao' + buyerId);
    var nick = activeUser.user_nick || uid;
    var cid = activeUser.cid || '';
    var securityUID = activeUser.securityUID || buyerId;
    var candidates = [
      ['qn.openChat activeUser', function(cb) {
        return invoke('qn.openChat', {
          uid: uid,
          nick: nick,
          cid: cid,
          securityUID: securityUID,
          targetId: securityUID,
          bizDomain: activeUser.bizDomain || 'taobao',
          bizType: activeUser.bizType || '11001'
        }, cb);
      }],
      ['qn.openChat cid', function(cb) {
        return invoke('qn.openChat', { cid: cid, bizDomain: activeUser.bizDomain || 'taobao' }, cb);
      }],
      ['qn.openChat nick', function(cb) {
        return invoke('qn.openChat', { nick: nick, bizDomain: activeUser.bizDomain || 'taobao' }, cb);
      }],
      ['qn.openChat targetId', function(cb) {
        return invoke('qn.openChat', { targetId: buyerId, bizDomain: 'taobao' }, cb);
      }],
      ['qn.openChat uid', function(cb) {
        return invoke('qn.openChat', { uid: 'cntaobao' + buyerId, bizDomain: 'taobao' }, cb);
      }],
      ['qn.openChat nickNumeric', function(cb) {
        return invoke('qn.openChat', { nick: 'cntaobao' + buyerId, bizDomain: 'taobao' }, cb);
      }],
      ['qn.imInsertText2Inputbox uid empty', function(cb) {
        return invoke('qn.imInsertText2Inputbox', { uid: uid, text: emptyText }, cb);
      }],
      ['imInsertText2Inputbox uid empty', function(cb) {
        return invoke('imInsertText2Inputbox', { uid: uid, text: emptyText }, cb);
      }],
      ['qn.imInsertText2Inputbox cid empty', function(cb) {
        return invoke('qn.imInsertText2Inputbox', { cid: cid, text: emptyText }, cb);
      }],
      ['qn.imInsertText2Inputbox uid draft', function(cb) {
        return invoke('qn.imInsertText2Inputbox', { uid: uid, text: draftText }, cb);
      }],
      ['imInsertText2Inputbox uid draft', function(cb) {
        return invoke('imInsertText2Inputbox', { uid: uid, text: draftText }, cb);
      }],
      ['qn.imInsertText2Inputbox cid draft', function(cb) {
        return invoke('qn.imInsertText2Inputbox', { cid: cid, text: draftText }, cb);
      }]
    ];

    candidates.forEach(function(candidate, index) {
      setTimeout(function() {
        callMaybePromise(candidate[0], candidate[1]);
      }, index * 700);
    });

    setTimeout(function() {
      probeObjectMethods('workbench.wangwang', workbench.wangwang, activeUser, draftText);
      probeObjectMethods('workbench.servicesummary', workbench.servicesummary, activeUser, draftText);
    }, candidates.length * 700 + 500);
  }

  function probeObjectMethods(label, obj, activeUser, draftText) {
    if (!obj) return;
    var keys = [];
    try { keys = Object.keys(obj); } catch (e) { return; }
    var interesting = keys.filter(function(k) {
      return /(open|chat|insert|input|text|send|msg|wangwang)/i.test(k) &&
        typeof obj[k] === 'function';
    }).slice(0, 20);
    reportProbe(label + ':interestingMethods', interesting);

    interesting.forEach(function(k, index) {
      setTimeout(function() {
        callMaybePromise(label + '.' + k + ' activeUser', function(cb) {
          return obj[k]({
            uid: activeUser.uid,
            nick: activeUser.user_nick || activeUser.uid,
            cid: activeUser.cid,
            securityUID: activeUser.securityUID,
            targetId: activeUser.securityUID,
            bizDomain: activeUser.bizDomain || 'taobao',
            bizType: activeUser.bizType || '11001',
            text: k.toLowerCase().includes('insert') || k.toLowerCase().includes('input') ? draftText : ''
          }, cb);
        });
      }, index * 500);
    });
  }

  setTimeout(function() {
    if (!DEBUG_PROBES) return;
    if (isNativeChatEventPage()) return;
    if (typeof workbench === 'undefined' ||
        !workbench.application ||
        typeof workbench.application.invoke !== 'function') {
      return;
    }

    var invoke = workbench.application.invoke.bind(workbench.application);
    describeObject('window.workbench', window.workbench);
    describeObject('workbench.application', workbench.application);
    describeObject('workbench.wangwang', workbench.wangwang);
    describeObject('workbench.servicesummary', workbench.servicesummary);
    describeObject('workbench.uiframemgr', workbench.uiframemgr);
    describeObject('workbench.top', workbench.top);
    describeObject('window.abilitycenter', window.abilitycenter);
    describeObject('abilitycenter.ability', window.abilitycenter && window.abilitycenter.ability);
    describeObject('window.AbilityPrivatecenter', window.AbilityPrivatecenter);
    describeObject('AbilityPrivatecenter.AbilityPrivate', window.AbilityPrivatecenter && window.AbilityPrivatecenter.AbilityPrivate);

    var tests = [
      ['invoke(getLoginuser,{})', function(cb) { return invoke('getLoginuser', {}, cb); }],
      ['invoke(getActiveUser,{})', function(cb) { return invoke('getActiveUser', {}, cb); }],
      ['invoke(object getLoginuser)', function(cb) { return invoke({ event: 'getLoginuser', param: {} }, cb); }],
      ['invoke(object getActiveUser)', function(cb) { return invoke({ event: 'getActiveUser', param: {} }, cb); }],
      ['invoke(qn.openChat noop)', function(cb) { return invoke('qn.openChat', {}, cb); }],
      ['invoke(getLoginuser,json)', function(cb) { return invoke('getLoginuser', '{}', cb); }],
      ['invoke(getActiveUser,json)', function(cb) { return invoke('getActiveUser', '{}', cb); }],
      ['invoke(bench,getLoginuser,{})', function(cb) { return invoke('bench', 'getLoginuser', {}, cb); }],
      ['invoke(bench,getActiveUser,{})', function(cb) { return invoke('bench', 'getActiveUser', {}, cb); }],
      ['invoke(12275,getActiveUser,{})', function(cb) { return invoke('12275', 'getActiveUser', {}, cb); }]
    ];

    tests.forEach(function(test, index) {
      setTimeout(function() {
        callMaybePromise(test[0], test[1]);
      }, index * 600);
    });
  }, 2500);

  // ─── 买家缓存管理 — 复刻 inject.js 第 130-196 行 ───

  function getCacheConv(ccode) {
    try {
      if (!window._buyerCache.has(ccode)) {
        updateBuyerCacheFromLocal();
      }
      return window._buyerCache.get(ccode);
    } catch (e) {
      console.error('[OpenBot] getCacheConv 错误:', e.message);
    }
    return undefined;
  }

  function updateFromConversation(conv) {
    try {
      if (!conv) return;
      if (!window._buyerCache.has(conv.ccode)) {
        window._buyerCache.set(conv.ccode, conv);
      }
      window.__openbotMacState.activeUser = {
        cid: conv.ccode || '',
        uid: conv.nick ? ('cntaobao' + conv.nick) : '',
        user_nick: conv.nick || conv.display || '',
        dnick: conv.display || conv.nick || '',
        securityUID: conv.targetId || '',
        bizDomain: 'taobao',
        bizType: conv.bizeType || conv.bizType || '11001'
      };
      if (conv.ccode) {
        window.__openbotMacState.activeUsers[conv.ccode] = window.__openbotMacState.activeUser;
      }
      if (conv.targetId) {
        window.__openbotMacState.buyerId = String(conv.targetId);
      }
    } catch (e) {
      console.error('[OpenBot] updateFromConversation 错误:', e.message);
    }
  }

  async function getRemoteMsg(ccode) {
    try {
      var remoteMsg = await withTimeout(imsdk.invoke('im.singlemsg.GetRemoteHisMsg', {
        cid: { ccode: ccode, type: 1 },
        count: 3,
        gohistory: 1,
        msgid: '-1',
        msgtime: '-1',
      }), 2500, 'GetRemoteHisMsg');
      var buyer = { ccode: ccode };
      var msgs = remoteMsg && remoteMsg.result && Array.isArray(remoteMsg.result.msgs)
        ? remoteMsg.result.msgs
        : [];
      for (var idx = 0; idx < msgs.length; idx++) {
        if (msgs[idx].loginid.nick !== msgs[idx].fromid.nick) {
          buyer = msgs[idx].fromid;
          break;
        }
      }
      return buyer;
    } catch (e) {
      console.error('[OpenBot] getRemoteMsg 错误:', e.message);
      return { ccode: ccode };
    }
  }

  async function getRemoteMessages(ccode) {
    try {
      var remoteMsg = await withTimeout(imsdk.invoke('im.singlemsg.GetRemoteHisMsg', {
        cid: { ccode: ccode, type: 1 },
        count: 5,
        gohistory: 1,
        msgid: '-1',
        msgtime: '-1'
      }), 2500, 'GetRemoteHisMsg');
      var result = remoteMsg && remoteMsg.result ? remoteMsg.result : {};
      var msgs = [];
      if (Array.isArray(remoteMsg)) {
        msgs = remoteMsg;
      } else if (Array.isArray(result)) {
        msgs = result;
      } else if (Array.isArray(result.msgs)) {
        msgs = result.msgs;
      } else if (remoteMsg && remoteMsg.data && Array.isArray(remoteMsg.data.msgs)) {
        msgs = remoteMsg.data.msgs;
      } else if (remoteMsg && remoteMsg.retdata && Array.isArray(remoteMsg.retdata.msgs)) {
        msgs = remoteMsg.retdata.msgs;
      }
      msgs.forEach(function(msg) {
        if (msg && !msg.ccode) msg.ccode = ccode;
      });
      sendMessage('workbenchProbe', {
        name: 'im.remoteMessages',
        value: {
          ccode: ccode,
          count: msgs.length,
          code: remoteMsg && remoteMsg.code,
          subcode: remoteMsg && remoteMsg.subcode,
          sampleKeys: msgs[0] ? Object.keys(msgs[0]).slice(0, 40) : []
        },
        href: String(location.href)
      });
      if (!msgs.length) {
        sendMessage('workbenchProbe', {
          name: 'im.remoteMessages:empty',
          value: {
            ccode: ccode,
            code: remoteMsg && remoteMsg.code,
            subcode: remoteMsg && remoteMsg.subcode,
            resultKeys: result ? Object.keys(result).slice(0, 40) : []
          },
          href: String(location.href)
        });
      }
      return {
        code: remoteMsg && typeof remoteMsg.code !== 'undefined' ? remoteMsg.code : 0,
        subcode: remoteMsg && typeof remoteMsg.subcode !== 'undefined' ? remoteMsg.subcode : 0,
        result: msgs
      };
    } catch (e) {
      sendMessage('workbenchProbe', {
        name: 'im.remoteMessages:error',
        value: { ccode: ccode, error: String(e && e.message || e) },
        href: String(location.href)
      });
      return { code: -1, subcode: 0, result: [] };
    }
  }

  function updateBuyerCacheFromLocal() {
    try {
      if (!window._db || !window._db.msgDataMap) return;
      var msgDataMap = window._db.msgDataMap;
      Array.from(msgDataMap).forEach(function(entry) {
        var ccode = entry[0];
        var messages = entry[1];
        if (window._buyerCache.has(ccode)) return;

        for (var i = 0; i < messages.length; i++) {
          var message = messages[i];
          var ext = message.ext || {};
          var originBanamaMessage = message.originBanamaMessage || {};
          var receiverNick = ext.receiver_nick;
          var senderNick = ext.sender_nick;

          if (!senderNick || !receiverNick) continue;

          if (senderNick.includes(window._vs.loginID.nick)) {
            window._buyerCache.set(ccode, originBanamaMessage.toid);
          }
          if (receiverNick.includes(window._vs.loginID.nick)) {
            window._buyerCache.set(ccode, originBanamaMessage.fromid);
          }
          break;
        }
      });
    } catch (e) {
      console.error('[OpenBot] updateBuyerCacheFromLocal 错误:', e.message);
    }
  }

  // ─── 启动 ───
  setupWebSocket();
  console.log('[OpenBot] macOS 注入脚本已加载');

})();
