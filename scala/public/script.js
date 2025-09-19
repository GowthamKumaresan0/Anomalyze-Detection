// Elements
const $ = (id) => document.getElementById(id);
const levelEl = $("level");
const textEl = $("text");
const keyEl = $("key");
const btn = $("sendBtn");
const statusEl = $("status");
const preview = $("preview");

// Helpers
function isoNow() {
  return new Date().toISOString().replace("T", " ").substring(0, 19);
}
function sampleLine() {
  return `${isoNow()}, ${levelEl.value}, ${textEl.value.trim() || "Sample event from Web UI"}`;
}
function showPreview() {
  preview.style.display = "block";
  preview.textContent = sampleLine();
}
function setStatus(msg, cls = "") {
  statusEl.textContent = msg;
  statusEl.className = `status ${cls}`.trim();
}

// Live preview
["change", "keyup"].forEach((ev) => {
  levelEl.addEventListener(ev, showPreview);
  textEl.addEventListener(ev, showPreview);
});
showPreview();

// Persist last used level/key in localStorage
(function restorePrefs() {
  const savedLevel = localStorage.getItem("log_level");
  const savedKey = localStorage.getItem("kafka_key");
  if (savedLevel) levelEl.value = savedLevel;
  if (savedKey) keyEl.value = savedKey;
  showPreview();
})();
levelEl.addEventListener("change", () => localStorage.setItem("log_level", levelEl.value));
keyEl.addEventListener("change", () => localStorage.setItem("kafka_key", keyEl.value));

// Submit
btn.onclick = async () => {
  const level = levelEl.value || "INFO";
  const text = textEl.value.trim();
  const key = (keyEl.value || "web").trim();

  if (!text) {
    // Allow empty message? You can relax this if you want.
    setStatus("Please enter a message to send.", "err");
    textEl.focus();
    return;
  }

  btn.disabled = true;
  setStatus("Sending...", "");

  try {
    const res = await fetch("/produce", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ level, text, key }),
    });

    const data = await res.json();
    if (res.ok && data.ok) {
      setStatus(`Sent ✔ ${data.sent}`, "ok");
      textEl.value = "";
      showPreview();
    } else {
      setStatus(`Error: ${data.error || res.statusText}`, "err");
    }
  } catch (e) {
    setStatus(`Network error: ${e.message}`, "err");
  } finally {
    btn.disabled = false;
  }
};

// Keyboard shortcut: Ctrl/Cmd + Enter to send
document.addEventListener("keydown", (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
    btn.click();
  }
});
