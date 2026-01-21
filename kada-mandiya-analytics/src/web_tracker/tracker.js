/* Kada Mandiya Web Tracker (Phase 1)
   - Auto page_view + click
   - Session id in localStorage w/ 30-min inactivity TTL
   - Best-effort user_id attachment
   - Async, non-blocking delivery; errors swallowed
*/

(function () {
  "use strict";

  if (typeof window === "undefined" || typeof document === "undefined") return;
  if (window.__KM_WEB_TRACKER_LOADED__) return;
  window.__KM_WEB_TRACKER_LOADED__ = true;

  var ANALYTICS_KEY = "__KM_ANALYTICS_WEB_KEY__";
  if (!ANALYTICS_KEY || ANALYTICS_KEY === "__KM_ANALYTICS_WEB_KEY__") {
    ANALYTICS_KEY = null;
  }

  var SESSION_STORAGE_KEY = "km_analytics_session";
  var PREV_PAGE_KEY = "km_analytics_prev_page";
  var SESSION_TTL_MS = 30 * 60 * 1000;

  function nowMs() {
    return Date.now();
  }

  function utcIso() {
    return new Date().toISOString();
  }

  function safeJsonParse(value) {
    try {
      return JSON.parse(value);
    } catch (_) {
      return null;
    }
  }

  function isProbablyEmail(value) {
    return typeof value === "string" && value.indexOf("@") !== -1;
  }

  function safeUserId(value) {
    if (value == null) return null;
    var s = String(value).trim();
    if (!s) return null;
    if (s.length > 64) return null;
    if (isProbablyEmail(s)) return null;
    return s;
  }

  function resolveUserId() {
    try {
      var direct = safeUserId(window.localStorage.getItem("user_id"));
      if (direct) return direct;
      direct = safeUserId(window.localStorage.getItem("userId"));
      if (direct) return direct;
      direct = safeUserId(window.localStorage.getItem("km_user_id"));
      if (direct) return direct;

      var userJson =
        window.localStorage.getItem("user") || window.localStorage.getItem("auth_user");
      if (userJson) {
        var obj = safeJsonParse(userJson);
        if (obj && typeof obj === "object") {
          var candidate =
            obj.user_id ?? obj.userId ?? obj.id ?? (obj.user && (obj.user.id ?? obj.user.user_id));
          var parsed = safeUserId(candidate);
          if (parsed) return parsed;
        }
      }
    } catch (_) {
      // ignore
    }
    return null;
  }

  function uuidv4() {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
      return window.crypto.randomUUID();
    }
    // Fallback (RFC4122-ish): not cryptographically strong, but sufficient for anonymous session ids.
    var d = nowMs();
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
      var r = (d + Math.random() * 16) % 16 | 0;
      d = Math.floor(d / 16);
      return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
    });
  }

  function getOrCreateSessionId() {
    var ts = nowMs();
    try {
      var raw = window.localStorage.getItem(SESSION_STORAGE_KEY);
      var data = raw ? safeJsonParse(raw) : null;
      if (data && typeof data === "object") {
        var sid = typeof data.id === "string" ? data.id : null;
        var last = typeof data.last_activity_ms === "number" ? data.last_activity_ms : null;
        if (sid && last && ts - last <= SESSION_TTL_MS) {
          data.last_activity_ms = ts;
          window.localStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(data));
          return sid;
        }
      }
      var newId = uuidv4();
      window.localStorage.setItem(
        SESSION_STORAGE_KEY,
        JSON.stringify({ id: newId, created_at_ms: ts, last_activity_ms: ts })
      );
      return newId;
    } catch (_) {
      return uuidv4();
    }
  }

  function currentScriptOrigin() {
    try {
      var s = document.currentScript;
      if (s && s.src) return new URL(s.src).origin;
    } catch (_) {
      // ignore
    }
    return null;
  }

  function collectorEventsUrl() {
    // Prefer the origin serving the tracker to avoid mixed-host confusion.
    var origin = currentScriptOrigin();
    if (!origin) origin = "http://127.0.0.1:8000";
    return origin.replace(/\/+$/, "") + "/events";
  }

  var queue = [];
  var flushTimer = null;
  var FLUSH_INTERVAL_MS = 500;
  var MAX_BATCH = 20;

  function scheduleFlush() {
    if (flushTimer != null) return;
    flushTimer = window.setTimeout(function () {
      flushTimer = null;
      flush(false);
    }, FLUSH_INTERVAL_MS);
  }

  function flush(keepalive) {
    if (!ANALYTICS_KEY) return;
    if (!queue.length) return;

    var batch = queue.splice(0, MAX_BATCH);
    try {
      window.fetch(collectorEventsUrl(), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-ANALYTICS-KEY": ANALYTICS_KEY,
        },
        body: JSON.stringify({ events: batch }),
        keepalive: !!keepalive,
      }).catch(function () {
        // swallow
      });
    } catch (_) {
      // swallow
    }

    if (queue.length) scheduleFlush();
  }

  function safeElementId(target) {
    try {
      if (!target || !target.getAttribute) return null;
      var explicit = target.getAttribute("data-analytics-id");
      if (explicit && String(explicit).trim()) return String(explicit).trim().slice(0, 100);

      if (target.id && String(target.id).trim()) return ("id:" + String(target.id).trim()).slice(0, 100);
      var nameAttr = target.getAttribute("name");
      if (nameAttr && String(nameAttr).trim())
        return ("name:" + String(nameAttr).trim()).slice(0, 100);

      var tag = (target.tagName || "UNKNOWN").toLowerCase();
      if (tag === "input" || tag === "textarea" || tag === "select") return tag;

      var text = (target.textContent || "").replace(/\s+/g, " ").trim();
      if (text && text.length <= 40 && text.indexOf("@") === -1) {
        return (tag + ":" + text).slice(0, 100);
      }
      return tag;
    } catch (_) {
      return null;
    }
  }

  function pageViewPayload(kind) {
    var sid = getOrCreateSessionId();
    var uid = resolveUserId();
    var url = String(window.location.href);

    var referrer = null;
    var timeOnPrev = null;
    try {
      var prevRaw = window.sessionStorage.getItem(PREV_PAGE_KEY);
      var prev = prevRaw ? safeJsonParse(prevRaw) : null;
      if (prev && typeof prev === "object" && typeof prev.url === "string") {
        referrer = prev.url;
        if (typeof prev.ts === "number") {
          var deltaSeconds = Math.floor((nowMs() - prev.ts) / 1000);
          if (deltaSeconds >= 0 && deltaSeconds <= 24 * 60 * 60) timeOnPrev = deltaSeconds;
        }
      } else if (document.referrer && String(document.referrer).trim()) {
        referrer = String(document.referrer).trim();
      }

      window.sessionStorage.setItem(PREV_PAGE_KEY, JSON.stringify({ url: url, ts: nowMs() }));
    } catch (_) {
      // ignore
    }

    var utmSource = null;
    var utmMedium = null;
    var utmCampaign = null;
    try {
      var u = new URL(url);
      utmSource = u.searchParams.get("utm_source");
      utmMedium = u.searchParams.get("utm_medium");
      utmCampaign = u.searchParams.get("utm_campaign");
    } catch (_) {
      // ignore
    }

    return {
      event_type: "page_view",
      event_timestamp: utcIso(),
      session_id: sid,
      user_id: uid,
      source: "web",
      page_url: url,
      referrer_url: referrer,
      utm_source: utmSource,
      utm_medium: utmMedium,
      utm_campaign: utmCampaign,
      time_on_prev_page_seconds: timeOnPrev,
      properties: {
        kind: kind || "unknown",
        viewport_w: window.innerWidth || null,
        viewport_h: window.innerHeight || null,
      },
    };
  }

  function clickPayload(evt) {
    var sid = getOrCreateSessionId();
    var uid = resolveUserId();
    var url = String(window.location.href);

    var x = null;
    var y = null;
    try {
      if (evt && typeof evt.clientX === "number") x = Math.round(evt.clientX);
      if (evt && typeof evt.clientY === "number") y = Math.round(evt.clientY);
    } catch (_) {
      // ignore
    }

    var target = evt && evt.target ? evt.target : null;
    // Avoid capturing text from deep children; prefer nearest element node
    try {
      if (target && target.nodeType !== 1 && evt.composedPath) {
        var path = evt.composedPath();
        for (var i = 0; i < path.length; i++) {
          if (path[i] && path[i].nodeType === 1) {
            target = path[i];
            break;
          }
        }
      }
    } catch (_) {
      // ignore
    }

    return {
      event_type: "click",
      event_timestamp: utcIso(),
      session_id: sid,
      user_id: uid,
      source: "web",
      page_url: url,
      element_id: safeElementId(target),
      x: x,
      y: y,
      viewport_w: window.innerWidth || null,
      viewport_h: window.innerHeight || null,
      user_agent: navigator && navigator.userAgent ? String(navigator.userAgent).slice(0, 300) : null,
      properties: null,
    };
  }

  function track(eventObj) {
    try {
      if (!eventObj || typeof eventObj !== "object") return;
      if (!ANALYTICS_KEY) return;
      queue.push(eventObj);
      scheduleFlush();
    } catch (_) {
      // swallow
    }
  }

  function onLocationChange(kind) {
    track(pageViewPayload(kind));
  }

  // Initial page view (defer => DOM parsed). Slight delay avoids racing initial route hydration.
  window.setTimeout(function () {
    onLocationChange("load");
  }, 0);

  // SPA navigation (history + back/forward)
  try {
    var origPushState = history.pushState;
    history.pushState = function () {
      var ret = origPushState.apply(this, arguments);
      window.dispatchEvent(new Event("km:locationchange"));
      return ret;
    };
    var origReplaceState = history.replaceState;
    history.replaceState = function () {
      var ret = origReplaceState.apply(this, arguments);
      window.dispatchEvent(new Event("km:locationchange"));
      return ret;
    };
    window.addEventListener("popstate", function () {
      window.dispatchEvent(new Event("km:locationchange"));
    });
    window.addEventListener("hashchange", function () {
      window.dispatchEvent(new Event("km:locationchange"));
    });
    window.addEventListener("km:locationchange", function () {
      onLocationChange("spa");
    });
  } catch (_) {
    // ignore
  }

  // Global click capture (no form values)
  document.addEventListener(
    "click",
    function (evt) {
      track(clickPayload(evt));
    },
    { capture: true, passive: true }
  );

  // Best-effort flush on background/unload
  document.addEventListener("visibilitychange", function () {
    if (document.visibilityState === "hidden") flush(true);
  });
  window.addEventListener("pagehide", function () {
    flush(true);
  });
})();

