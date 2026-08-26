// Open the downloader in a normal browser tab when the toolbar icon is clicked.
// If our tab is already open, focus it instead of spawning another.
let tabId = null;

chrome.action.onClicked.addListener(async () => {
  // Always open a clean page — no auto-loading of the last watched video.
  const target = chrome.runtime.getURL("window.html");

  // Reuse the existing tab if it's still open.
  if (tabId !== null) {
    try {
      const t = await chrome.tabs.get(tabId);
      await chrome.tabs.update(tabId, { active: true });
      if (t && t.windowId != null) await chrome.windows.update(t.windowId, { focused: true });
      return;
    } catch (e) {
      tabId = null; // it was closed
    }
  }

  try {
    const t = await chrome.tabs.create({ url: target });
    tabId = t.id;
  } catch (e) {
    console.warn("tabs.create:", e);
  }
});

chrome.tabs.onRemoved.addListener((id) => {
  if (id === tabId) tabId = null;
});
