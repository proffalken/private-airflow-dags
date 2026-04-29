'use strict'

const $ = id => document.getElementById(id)

async function load() {
  const data = await chrome.storage.local.get([
    'backendUrl', 'username', 'password', 'syncIntervalMinutes',
  ])
  $('backendUrl').value = data.backendUrl || 'https://social-archive.wallace.network'
  // backendUrl is the frontend host — the extension routes through /api/ext/*
  $('username').value = data.username || ''
  $('password').value = data.password || ''
  $('syncInterval').value = data.syncIntervalMinutes || 60
}

$('save').addEventListener('click', async () => {
  const backendUrl = $('backendUrl').value.replace(/\/$/, '')
  const username = $('username').value.trim()
  const password = $('password').value
  const syncIntervalMinutes = Math.max(10, parseInt($('syncInterval').value, 10) || 60)

  if (!backendUrl || !username || !password) {
    showStatus('All fields are required.', true)
    return
  }

  await chrome.storage.local.set({
    backendUrl,
    username,
    password,
    syncIntervalMinutes,
    token: null, // invalidate cached token on credential change
  })

  // Tell the service worker to reschedule the alarm
  chrome.runtime.sendMessage({ type: 'RESCHEDULE' })

  showStatus('Saved. Sync will run every ' + syncIntervalMinutes + ' minutes.')
})

function showStatus(msg, isError = false) {
  const el = $('status')
  el.textContent = msg
  el.className = isError ? 'error' : ''
}

load()
