document.getElementById("eventForm").addEventListener("submit", async function(e) {
  e.preventDefault();

  const eventsInput = document.getElementById("events").value;
  const events = eventsInput.split(",").map(e => e.trim());

  const payload = {
    events: events,
    label: document.getElementById("label").value || "Success",
                                                      key: document.getElementById("key").value || "web"
  };

  const responseBox = document.getElementById("response");
  responseBox.textContent = "Sending...";
  responseBox.className = "";

  try {
    const res = await fetch("/produce", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    if (!res.ok) throw new Error("Failed to send event");

    const data = await res.json();
    responseBox.textContent = JSON.stringify(data, null, 2);
    responseBox.classList.add("success");
  } catch (err) {
    responseBox.textContent = "Error: " + err.message;
    responseBox.classList.add("error");
  }
});
