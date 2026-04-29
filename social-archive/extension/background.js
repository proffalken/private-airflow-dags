'use strict'

const ALARM_NAME = 'bookmark-sync'
const DEFAULT_INTERVAL_MINUTES = 60

// ── Browser detection ────────────────────────────────────────────────────────

async function detectBrowser() {
  try {
    if (self.navigator?.brave && await self.navigator.brave.isBrave()) {
      return 'brave'
    }
  } catch (_) {}
  return 'chrome'
}

// ── Bookmark tree flattening ─────────────────────────────────────────────────

function flattenBookmarks(nodes, ancestorFolders = []) {
  const results = []
  for (const node of nodes) {
    if (node.url) {
      // Leaf node — it's a bookmark
      results.push({
        title: node.title || null,
        uri: node.url,
        source_context: ancestorFolders.length > 0
          ? ancestorFolders[ancestorFolders.length - 1]
          : null,
        tags: [...ancestorFolders],
      })
    } else if (node.children) {
      // Folder node — skip the two synthetic root nodes ("Bookmarks bar", "Other bookmarks")
      // which have no real parent and would produce noise as tags
      const folderName = node.parentId ? node.title : null
      const nextAncestors = folderName
        ? [...ancestorFolders, folderName]
        : ancestorFolders
      results.push(...flattenBookmarks(node.children, nextAncestors))
    }
  }
  return results
}

// ── Auth ─────────────────────────────────────────────────────────────────────

async function getToken(backendUrl, username, password) {
  // Uses /api/ext/login which returns the token in the body (unlike the
  // browser login route which sets an httpOnly cookie and returns {ok:true})
  const res = await fetch(`${backendUrl}/api/ext/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  })
  if (!res.ok) throw new Error(`Login failed: ${res.status}`)
  const data = await res.json()
  return data.access_token
}

// ── Core sync ────────────────────────────────────────────────────────────────

async function runSync() {
  const { backendUrl, username, password, token: cachedToken } =
    await chrome.storage.local.get(['backendUrl', 'username', 'password', 'token'])

  if (!backendUrl || !username || !password) {
    console.warn('[social-archive] Extension not configured — skipping sync')
    return { status: 'unconfigured' }
  }

  const source = await detectBrowser()

  // Flatten the full bookmark tree
  const tree = await chrome.bookmarks.getTree()
  const bookmarks = flattenBookmarks(tree).map(b => ({ ...b, source }))

  if (bookmarks.length === 0) {
    return { status: 'ok', inserted: 0, skipped: 0 }
  }

  // Try with cached token first, re-auth on 401
  let token = cachedToken
  for (let attempt = 0; attempt < 2; attempt++) {
    if (!token) {
      token = await getToken(backendUrl, username, password)
      await chrome.storage.local.set({ token })
    }

    const res = await fetch(`${backendUrl}/api/ext/bookmarks/sync`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`,
      },
      body: JSON.stringify({ bookmarks }),
    })

    if (res.status === 401) {
      token = null // force re-auth on next attempt
      continue
    }
    if (!res.ok) {
      let detail = `${res.status}`
      try {
        const errBody = await res.json()
        detail = errBody.detail || errBody.error || JSON.stringify(errBody)
      } catch (_) {}
      throw new Error(`Sync failed (${res.status}): ${detail}`)
    }

    const result = await res.json()
    const now = new Date().toISOString()
    await chrome.storage.local.set({
      lastSyncAt: now,
      lastSyncInserted: result.inserted,
      lastSyncSkipped: result.skipped,
      lastSyncError: null,
    })
    console.log(`[social-archive] Sync complete — inserted: ${result.inserted}, skipped: ${result.skipped}`)
    return { status: 'ok', ...result }
  }

  throw new Error('Auth failed after retry')
}

// ── Alarm management ─────────────────────────────────────────────────────────

async function scheduleAlarm() {
  const { syncIntervalMinutes = DEFAULT_INTERVAL_MINUTES } =
    await chrome.storage.local.get('syncIntervalMinutes')
  await chrome.alarms.clearAll()
  chrome.alarms.create(ALARM_NAME, {
    delayInMinutes: syncIntervalMinutes,
    periodInMinutes: syncIntervalMinutes,
  })
}

// ── Event listeners ──────────────────────────────────────────────────────────

chrome.runtime.onInstalled.addListener(async () => {
  await scheduleAlarm()
})

chrome.alarms.onAlarm.addListener(async (alarm) => {
  if (alarm.name !== ALARM_NAME) return
  try {
    await runSync()
  } catch (err) {
    console.error('[social-archive] Sync error:', err)
    await chrome.storage.local.set({ lastSyncError: err.message })
  }
})

// Message handler so popup can trigger a manual sync or reschedule
chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg.type === 'SYNC_NOW') {
    runSync()
      .then(sendResponse)
      .catch(err => sendResponse({ status: 'error', message: err.message }))
    return true // keep channel open for async response
  }
  if (msg.type === 'RESCHEDULE') {
    scheduleAlarm().then(() => sendResponse({ status: 'ok' }))
    return true
  }
})
