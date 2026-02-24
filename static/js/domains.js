/* ================================================================
   domains.js — Domain Management Dashboard
   ================================================================ */
(function () {
  "use strict";

  const API = "/api/admin/domains";
  let currentPage = 1;
  let currentSearch = "";
  let deleteId = null;
  let debounceTimer = null;

  // ── Elements ─────────────────────────────────────────────────
  const searchInput = document.getElementById("search-input");
  const addBtn = document.getElementById("add-btn");
  const tbody = document.getElementById("domain-tbody");
  const emptyState = document.getElementById("empty-state");
  const loadingState = document.getElementById("loading-state");
  const totalCount = document.getElementById("total-count");
  const pageInfo = document.getElementById("page-info");
  const pagination = document.getElementById("pagination");

  // Modal elements
  const modalOverlay = document.getElementById("modal-overlay");
  const modalTitle = document.getElementById("modal-title");
  const modalClose = document.getElementById("modal-close");
  const modalCancel = document.getElementById("modal-cancel");
  const domainForm = document.getElementById("domain-form");
  const formError = document.getElementById("form-error");
  const submitLabel = document.getElementById("submit-label");
  const fId = document.getElementById("f-id");
  const fCode = document.getElementById("f-code");
  const fName = document.getElementById("f-name");
  const fApiUrl = document.getElementById("f-api-url");
  const fUsername = document.getElementById("f-username");
  const fUsertoken = document.getElementById("f-usertoken");
  const fLeDomain = document.getElementById("f-le-domain");
  const fPhase = document.getElementById("f-phase");
  const fEnabled = document.getElementById("f-enabled");

  // Delete modal
  const deleteOverlay = document.getElementById("delete-overlay");
  const deleteClose = document.getElementById("delete-close");
  const deleteCancel = document.getElementById("delete-cancel");
  const deleteConfirm = document.getElementById("delete-confirm");
  const deleteName = document.getElementById("delete-name");

  const toastContainer = document.getElementById("toast-container");

  // ── Helpers ──────────────────────────────────────────────────
  function showToast(msg, type = "success") {
    const t = document.createElement("div");
    t.className = `toast toast-${type}`;
    t.textContent = msg;
    toastContainer.appendChild(t);
    requestAnimationFrame(() => t.classList.add("show"));
    setTimeout(() => {
      t.classList.remove("show");
      setTimeout(() => t.remove(), 300);
    }, 3000);
  }

  function truncate(str, max = 30) {
    return str && str.length > max ? str.slice(0, max) + "…" : str || "";
  }

  // ── Fetch domains ───────────────────────────────────────────
  async function fetchDomains(page = 1, search = "") {
    currentPage = page;
    currentSearch = search;

    tbody.innerHTML = "";
    emptyState.style.display = "none";
    loadingState.style.display = "flex";

    try {
      const params = new URLSearchParams({ page, search });
      const res = await fetch(`${API}?${params}`);
      const data = await res.json();

      if (!data.success) throw new Error(data.error || "Failed to load");

      loadingState.style.display = "none";
      totalCount.textContent = data.total;
      pageInfo.textContent = `Page ${data.page} of ${data.total_pages}`;

      if (data.domains.length === 0) {
        emptyState.style.display = "flex";
        pagination.innerHTML = "";
        return;
      }

      renderTable(data.domains);
      renderPagination(data.page, data.total_pages);
    } catch (err) {
      loadingState.style.display = "none";
      emptyState.style.display = "flex";
      showToast(err.message, "error");
    }
  }

  // ── Render table rows ───────────────────────────────────────
  function renderTable(domains) {
    tbody.innerHTML = domains
      .map(
        (d) => `
      <tr>
        <td class="col-code"><span class="code-badge">${esc(d.code)}</span></td>
        <td class="col-name">${esc(d.name)}</td>
        <td class="col-domain">${esc(d.le_domain)}</td>
        <td class="col-user">${esc(d.username)}</td>
        <td class="col-phase"><span class="phase-badge">P${d.phase}</span></td>
        <td class="col-status">
          <span class="status-dot ${d.enabled ? "active" : "inactive"}"></span>
          ${d.enabled ? "Active" : "Disabled"}
        </td>
        <td class="col-actions">
          <button class="act-btn act-edit" data-id="${d.id}" title="Edit">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/>
              <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>
            </svg>
          </button>
          <button class="act-btn act-del" data-id="${d.id}" data-name="${esc(d.name)}" title="Delete">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <polyline points="3 6 5 6 21 6"/>
              <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
            </svg>
          </button>
        </td>
      </tr>`
      )
      .join("");

    // Attach row event listeners
    tbody.querySelectorAll(".act-edit").forEach((btn) =>
      btn.addEventListener("click", () => openEditModal(btn.dataset.id))
    );
    tbody.querySelectorAll(".act-del").forEach((btn) =>
      btn.addEventListener("click", () =>
        openDeleteModal(btn.dataset.id, btn.dataset.name)
      )
    );
  }

  function esc(s) {
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  // ── Pagination ──────────────────────────────────────────────
  function renderPagination(page, total) {
    if (total <= 1) {
      pagination.innerHTML = "";
      return;
    }
    let html = "";

    // Prev
    html += `<button class="pg-btn ${page === 1 ? "disabled" : ""}" data-page="${page - 1}" ${page === 1 ? "disabled" : ""}>
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="15 18 9 12 15 6"/></svg>
    </button>`;

    // Page numbers — show max 7 buttons
    const start = Math.max(1, page - 3);
    const end = Math.min(total, start + 6);
    for (let i = start; i <= end; i++) {
      html += `<button class="pg-btn ${i === page ? "active" : ""}" data-page="${i}">${i}</button>`;
    }

    // Next
    html += `<button class="pg-btn ${page === total ? "disabled" : ""}" data-page="${page + 1}" ${page === total ? "disabled" : ""}>
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="9 18 15 12 9 6"/></svg>
    </button>`;

    pagination.innerHTML = html;
    pagination.querySelectorAll(".pg-btn:not(.disabled)").forEach((btn) =>
      btn.addEventListener("click", () =>
        fetchDomains(parseInt(btn.dataset.page), currentSearch)
      )
    );
  }

  // ── Modal: Add ──────────────────────────────────────────────
  function openAddModal() {
    fId.value = "";
    domainForm.reset();
    fPhase.value = "2";
    fEnabled.checked = true;
    fApiUrl.value = "https://ib5131.pptsend.com/apixml.php";
    modalTitle.textContent = "Add Domain";
    submitLabel.textContent = "Save Domain";
    formError.style.display = "none";
    fCode.disabled = false;
    modalOverlay.classList.add("open");
  }

  // ── Modal: Edit ─────────────────────────────────────────────
  async function openEditModal(id) {
    try {
      const res = await fetch(`${API}/${id}`);
      const data = await res.json();
      if (!data.success) throw new Error(data.error);
      const d = data.domain;
      fId.value = d.id;
      fCode.value = d.code;
      fCode.disabled = true; // code is immutable after creation
      fName.value = d.name;
      fApiUrl.value = d.api_url;
      fUsername.value = d.username;
      fUsertoken.value = d.usertoken;
      fLeDomain.value = d.le_domain;
      fPhase.value = d.phase;
      fEnabled.checked = d.enabled;
      modalTitle.textContent = "Edit Domain";
      submitLabel.textContent = "Update Domain";
      formError.style.display = "none";
      modalOverlay.classList.add("open");
    } catch (err) {
      showToast(err.message, "error");
    }
  }

  function closeModal() {
    modalOverlay.classList.remove("open");
  }

  // ── Modal: Delete ───────────────────────────────────────────
  function openDeleteModal(id, name) {
    deleteId = id;
    deleteName.textContent = name;
    deleteOverlay.classList.add("open");
  }

  function closeDeleteModal() {
    deleteOverlay.classList.remove("open");
    deleteId = null;
  }

  // ── Form submit (create / update) ──────────────────────────
  async function handleSubmit(e) {
    e.preventDefault();
    formError.style.display = "none";

    const payload = {
      code: fCode.value.trim(),
      name: fName.value.trim(),
      api_url: fApiUrl.value.trim(),
      username: fUsername.value.trim(),
      usertoken: fUsertoken.value.trim(),
      le_domain: fLeDomain.value.trim(),
      phase: parseInt(fPhase.value) || 2,
      enabled: fEnabled.checked,
    };

    const isEdit = !!fId.value;
    const url = isEdit ? `${API}/${fId.value}` : API;
    const method = isEdit ? "PUT" : "POST";

    try {
      const res = await fetch(url, {
        method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!data.success) {
        formError.textContent = data.error;
        formError.style.display = "block";
        return;
      }
      closeModal();
      showToast(isEdit ? "Domain updated" : "Domain created");
      fetchDomains(isEdit ? currentPage : 1, currentSearch);
    } catch (err) {
      formError.textContent = err.message;
      formError.style.display = "block";
    }
  }

  // ── Delete confirm ─────────────────────────────────────────
  async function handleDelete() {
    if (!deleteId) return;
    try {
      const res = await fetch(`${API}/${deleteId}`, { method: "DELETE" });
      const data = await res.json();
      if (!data.success) throw new Error(data.error);
      closeDeleteModal();
      showToast("Domain deleted");
      fetchDomains(currentPage, currentSearch);
    } catch (err) {
      closeDeleteModal();
      showToast(err.message, "error");
    }
  }

  // ── Event listeners ────────────────────────────────────────
  searchInput.addEventListener("input", () => {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
      fetchDomains(1, searchInput.value.trim());
    }, 300);
  });

  addBtn.addEventListener("click", openAddModal);
  modalClose.addEventListener("click", closeModal);
  modalCancel.addEventListener("click", closeModal);
  domainForm.addEventListener("submit", handleSubmit);

  deleteClose.addEventListener("click", closeDeleteModal);
  deleteCancel.addEventListener("click", closeDeleteModal);
  deleteConfirm.addEventListener("click", handleDelete);

  // Close modals on overlay click
  modalOverlay.addEventListener("click", (e) => {
    if (e.target === modalOverlay) closeModal();
  });
  deleteOverlay.addEventListener("click", (e) => {
    if (e.target === deleteOverlay) closeDeleteModal();
  });

  // Escape key
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      closeModal();
      closeDeleteModal();
    }
  });

  // ── Init ───────────────────────────────────────────────────
  fetchDomains(1, "");
})();
