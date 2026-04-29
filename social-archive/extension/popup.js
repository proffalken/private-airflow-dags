'use strict'

const $ = id => document.getElementById(id)

async function refreshStatus() {
  const data = await chrome.storage.local.get([
    'lastSyncAt', 'lastSyncInserted', 'lastSyncSkipped', 'lastSyncError',
  ])

  $('lastSync').textContent = data.lastSyncAt
    ? new Date(data.lastSyncAt).toLocaleString()
    : 'never'
  $('inserted').textContent = data.lastSyncInserted ?? '—'
  $('skipped').textContent = data.lastSyncSkipped ?? '—'
  $('errorMsg').textContent = data.lastSyncError ?? ''
}

async function showBrowser() {
  let label = 'Chrome'
  let cls = 'chrome'
  try {
    if (navigator.brave && await navigator.brave.isBrave()) {
      label = 'Brave'
      cls = 'brave'
    }
  } catch (_) {}
  const badge = $('browserBadge')
  badge.textContent = label
  badge.className = `browser-badge ${cls}`
}

$('syncBtn').addEventListener('click', async () => {
  $('syncBtn').disabled = true
  $('syncBtn').textContent = 'Syncing…'
  $('errorMsg').textContent = ''

  try {
    const result = await chrome.runtime.sendMessage({ type: 'SYNC_NOW' })
    if (result?.status === 'error') {
      $('errorMsg').textContent = result.message
    } else if (result?.status === 'unconfigured') {
      $('errorMsg').textContent = 'Not configured — open Options first.'
    }
    await refreshStatus()
  } catch (err) {
    $('errorMsg').textContent = err.message
  } finally {
    $('syncBtn').disabled = false
    $('syncBtn').textContent = 'Sync now'
  }
})

$('optionsBtn').addEventListener('click', () => {
  chrome.runtime.openOptionsPage()
})

refreshStatus()
showBrowser()
