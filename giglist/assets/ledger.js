/* Tight Ledger enhancement layer — see DESIGN.md.
   Progressive: without this file the page is a complete static ledger.
   With it: the banner becomes the menu button and carries the week label
   in view; a sidebar offers venue show/highlight(blue)/highlight(grey)/
   hide cycling (persisted per region in localStorage) plus week/month/
   region/view navigation. */
(function () {
  "use strict";

  var dataEl = document.getElementById("gl-data");
  var banner = document.getElementById("banner");
  if (!dataEl || !banner) return;

  var DATA;
  try { DATA = JSON.parse(dataEl.textContent); } catch (e) { return; }
  var REGION = DATA.region || "";
  var WEEKS = DATA.weeks || [];
  var STORE_KEY = "gl:venues:" + REGION;
  var MONTHS = ["January", "February", "March", "April", "May", "June", "July",
                "August", "September", "October", "November", "December"];

  /* ---------- venue state ---------- */

  var CYCLE = { show: "hl", hl: "hl2", hl2: "hide", hide: "show" };
  var STATE_LABEL = { show: "shown", hl: "highlighted blue",
                      hl2: "highlighted grey", hide: "hidden" };

  var rows = [].slice.call(document.querySelectorAll(".row[data-venue]"));
  var venues = [];
  rows.forEach(function (r) {
    var v = r.dataset.venue;
    if (venues.indexOf(v) === -1) venues.push(v);
  });
  venues.sort(function (a, b) { return a.localeCompare(b); });

  var vstate = {};
  venues.forEach(function (v) { vstate[v] = "show"; });
  try {
    var saved = JSON.parse(localStorage.getItem(STORE_KEY) || "{}");
    venues.forEach(function (v) {
      if (saved[v] && CYCLE[saved[v]]) vstate[v] = saved[v];
    });
  } catch (e) { /* corrupt storage: fall through to all-shown */ }

  function persist() {
    var out = {};
    venues.forEach(function (v) { if (vstate[v] !== "show") out[v] = vstate[v]; });
    try {
      if (Object.keys(out).length) localStorage.setItem(STORE_KEY, JSON.stringify(out));
      else localStorage.removeItem(STORE_KEY);
    } catch (e) { /* storage unavailable: states last for the visit */ }
  }

  function applyStates() {
    rows.forEach(function (r) {
      var st = vstate[r.dataset.venue] || "show";
      r.classList.toggle("hl", st === "hl");
      r.classList.toggle("hl2", st === "hl2");
      r.classList.toggle("gl-hidden", st === "hide");
    });
    var anyVisible = false;
    [].forEach.call(document.querySelectorAll(".wk-sec"), function (sec) {
      var secVisible = false;
      [].forEach.call(sec.querySelectorAll(".day-h"), function (bar) {
        var dayVisible = false;
        var el = bar.nextElementSibling;
        while (el && el.classList.contains("row")) {
          if (!el.classList.contains("gl-hidden")) dayVisible = true;
          el = el.nextElementSibling;
        }
        bar.classList.toggle("gl-hidden", !dayVisible);
        if (dayVisible) secVisible = true;
      });
      sec.classList.toggle("gl-hidden", !secVisible);
      if (secVisible) anyVisible = true;
    });
    var note = document.querySelector(".empty-note");
    if (note) note.hidden = anyVisible;
    updateWkLabel();
  }

  /* ---------- sidebar construction ---------- */

  function el(tag, attrs, text) {
    var e = document.createElement(tag);
    for (var k in attrs) e.setAttribute(k, attrs[k]);
    if (text) e.textContent = text;
    return e;
  }

  function category(title, opened, extraBtn) {
    var h = el("h3", { class: "cat", role: "button", tabindex: "0",
                       "aria-expanded": String(opened) });
    h.appendChild(el("span", {}, title));
    var right = el("span", { class: "cat-r" });
    if (extraBtn) right.appendChild(extraBtn);
    right.appendChild(el("span", { class: "ind" }, opened ? "−" : "+"));
    h.appendChild(right);
    var ul = el("ul", {});
    if (!opened) ul.hidden = true;
    h.addEventListener("click", function (e) {
      if (extraBtn && e.target === extraBtn) return;
      var opening = ul.hidden;
      ul.hidden = !opening;
      h.setAttribute("aria-expanded", String(opening));
      h.querySelector(".ind").textContent = opening ? "−" : "+";
    });
    h.addEventListener("keydown", function (e) {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); h.click(); }
    });
    return { h: h, ul: ul };
  }

  function weekMonday(id) {
    var m = id.match(/^week-(\d{4})-(\d{2})-(\d{2})$/);
    return m ? new Date(+m[1], +m[2] - 1, +m[3]) : null;
  }

  function currentWeekId() {
    var now = new Date();
    var today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    for (var i = 0; i < WEEKS.length; i++) {
      var mon = weekMonday(WEEKS[i].id);
      if (!mon) continue;
      var sun = new Date(mon); sun.setDate(sun.getDate() + 6);
      if (today >= mon && today <= sun) return WEEKS[i].id;
    }
    return WEEKS.length ? WEEKS[0].id : null;
  }

  var sidebar = el("aside", { id: "sidebar", "aria-label": "Site navigation" });
  var head = el("div", { class: "sb-head" });
  head.appendChild(el("span", {}, "GIGLIST — " + REGION.toUpperCase()));
  var closeBtn = el("button", { id: "sb-close" }, "Close");
  head.appendChild(closeBtn);
  sidebar.appendChild(head);

  /* Venues (open by default, with reset) */
  var resetBtn = el("button", { id: "v-reset" }, "reset");
  var catV = category("Venues", true, resetBtn);
  catV.ul.id = "sb-venues";
  sidebar.appendChild(catV.h); sidebar.appendChild(catV.ul);

  function renderVenues() {
    catV.ul.textContent = "";
    venues.forEach(function (v) {
      var st = vstate[v];
      var li = el("li", { "data-state": st });
      var ico = el("button", { class: "v-ico", "data-state": st,
        "aria-label": v + ": " + STATE_LABEL[st] + ". Click to change." });
      ico.addEventListener("click", function () {
        vstate[v] = CYCLE[vstate[v]];
        li.setAttribute("data-state", vstate[v]);
        ico.setAttribute("data-state", vstate[v]);
        ico.setAttribute("aria-label", v + ": " + STATE_LABEL[vstate[v]] + ". Click to change.");
        persist(); applyStates();
      });
      li.appendChild(ico);
      li.appendChild(el("span", { class: "v-name" }, v));
      catV.ul.appendChild(li);
    });
  }
  resetBtn.addEventListener("click", function (e) {
    e.stopPropagation();
    venues.forEach(function (v) { vstate[v] = "show"; });
    persist(); renderVenues(); applyStates();
  });

  /* Weeks */
  var curWeek = currentWeekId();
  var catW = category("Weeks", false);
  WEEKS.forEach(function (w) {
    var li = el("li", {});
    var a = el("a", { href: "./#" + w.id },
               w.label.replace("WEEK OF ", "").toLowerCase());
    if (w.id === curWeek) {
      a.appendChild(document.createTextNode(" "));
      a.appendChild(el("span", { class: "now" }, "← this week"));
    }
    li.appendChild(a);
    catW.ul.appendChild(li);
  });
  sidebar.appendChild(catW.h); sidebar.appendChild(catW.ul);

  /* Months (first week of each month) */
  var catM = category("Months", false);
  var seenMonths = {};
  WEEKS.forEach(function (w) {
    var mon = weekMonday(w.id);
    if (!mon) return;
    var key = mon.getFullYear() + "-" + mon.getMonth();
    if (seenMonths[key]) return;
    seenMonths[key] = true;
    var li = el("li", {});
    li.appendChild(el("a", { href: "./#" + w.id },
      MONTHS[mon.getMonth()] + " " + mon.getFullYear()));
    catM.ul.appendChild(li);
  });
  sidebar.appendChild(catM.h); sidebar.appendChild(catM.ul);

  /* Region */
  var catR = category("Region", false);
  [["Minnesota", "/mn/", "mn"], ["Tennessee", "/tn/", "tn"]].forEach(function (r) {
    var li = el("li", {});
    var a = el("a", { href: r[1] }, r[0]);
    if (r[2] === REGION) {
      a.appendChild(document.createTextNode(" "));
      a.appendChild(el("span", { class: "now" }, "← here"));
    }
    li.appendChild(a);
    catR.ul.appendChild(li);
  });
  var liAll = el("li", {});
  liAll.appendChild(el("a", { href: "/" }, "All states →"));
  catR.ul.appendChild(liAll);
  sidebar.appendChild(catR.h); sidebar.appendChild(catR.ul);

  /* Views (last, per DESIGN.md) */
  var catVw = category("Views", false);
  [["Weekly view", curWeek ? curWeek + ".html" : "./"], ["Full list", "./"]]
    .forEach(function (v) {
      var li = el("li", {});
      li.appendChild(el("a", { href: v[1] }, v[0]));
      catVw.ul.appendChild(li);
    });
  sidebar.appendChild(catVw.h); sidebar.appendChild(catVw.ul);

  var scrim = el("div", { id: "scrim" });
  document.body.appendChild(scrim);
  document.body.appendChild(sidebar);
  renderVenues();

  /* ---------- open/close ---------- */

  function setSidebar(open) {
    sidebar.classList.toggle("open", open);
    scrim.classList.toggle("open", open);
    banner.setAttribute("aria-expanded", String(open));
  }
  banner.setAttribute("role", "button");
  banner.setAttribute("aria-controls", "sidebar");
  banner.setAttribute("aria-expanded", "false");
  banner.setAttribute("aria-label", "Menu");
  banner.addEventListener("click", function (e) {
    e.preventDefault();
    setSidebar(!sidebar.classList.contains("open"));
  });
  closeBtn.addEventListener("click", function () { setSidebar(false); });
  scrim.addEventListener("click", function () { setSidebar(false); });
  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape") setSidebar(false);
  });

  /* ---------- banner week label follows the week in view ---------- */

  var rlabel = banner.querySelector(".rlabel");
  function updateWkLabel() {
    if (!rlabel) return;
    var secs = document.querySelectorAll(".wk-sec:not(.gl-hidden)");
    if (!secs.length) { rlabel.textContent = ""; return; }
    var label = secs[0].dataset.label || "";
    var y = banner.offsetHeight + 12;
    [].forEach.call(secs, function (s) {
      if (s.getBoundingClientRect().top <= y) label = s.dataset.label || label;
    });
    rlabel.textContent = label;
  }
  document.body.classList.add("wk-banner");
  addEventListener("scroll", updateWkLabel, { passive: true });
  addEventListener("resize", updateWkLabel);
  // Scroll events can be throttled or (in some embedded browsers) not
  // delivered for programmatic scrolls; an IntersectionObserver on the
  // week sections keeps the banner label honest regardless.
  if ("IntersectionObserver" in window) {
    var io = new IntersectionObserver(updateWkLabel, {
      rootMargin: "0px 0px -60% 0px", threshold: [0, 0.01, 1],
    });
    [].forEach.call(document.querySelectorAll(".wk-sec"), function (s) {
      io.observe(s);
    });
  }

  applyStates();
})();
