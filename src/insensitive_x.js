// Based on "X (Twitter) Insensitive & Age Bypass Advanced" v2.0.0 by Alican Aktas (MIT license),
// https://greasyfork.org/tr/users/1630721-alican-akta%C5%9F
// Injected by the bot into every page before X's own scripts run. It rewrites X's API (JSON) answers so
// that sensitive-content flags and warning screens are removed. Safe to run twice (guarded).
(function () {
  'use strict';
  if (window.__insensitiveX || typeof window.fetch !== 'function') return;
  window.__insensitiveX = true;
  const originalFetch = window.fetch;

  const clean = (obj) => {
    if (obj === null || typeof obj !== 'object') return;
    if (Array.isArray(obj)) {
      for (let i = 0; i < obj.length; i++) clean(obj[i]);
      return;
    }
    for (const key in obj) {
      if (!Object.prototype.hasOwnProperty.call(obj, key)) continue;
      if (key === 'possibly_sensitive' || key === 'sensitive_media') obj[key] = false;
      else if (key === 'tweet_interstitial' || key === 'tombstone') delete obj[key];
      else clean(obj[key]);
    }
  };

  window.fetch = async function (...args) {
    const response = await originalFetch.apply(this, args);
    const first = args[0];
    const url = typeof first === 'string' ? first : (first && first.url) || '';
    if (url.includes('/graphql/') || url.includes('/1.1/')) {
      try {
        const clone = response.clone();
        if ((clone.headers.get('content-type') || '').includes('application/json')) {
          const json = await clone.json();
          clean(json);
          return new Response(JSON.stringify(json), {
            status: response.status,
            statusText: response.statusText,
            headers: response.headers,
          });
        }
      } catch (e) {
        // If anything is unsupported in this browser, fall back to the untouched response.
      }
    }
    return response;
  };
})();
