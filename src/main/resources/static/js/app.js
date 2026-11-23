/*
 * EV 충전 빈자리 검색 화면.
 * - 지도 영역 검색: 지도를 멈출 때마다 화면 중심과 보이는 범위로 반경 검색 (최대 10km)
 * - 키워드 검색: 이름·주소로 찾고 결과에 맞춰 지도를 옮김
 * - 실시간: 빈자리 이벤트(SSE)를 받으면 해당 충전소를 바로 다시 읽고, 그 외 변화는 2분마다 다시 읽음.
 *   다시 연결되거나 탭으로 돌아오면 그 사이 놓친 변화를 맞추려고 바로 다시 읽음 (ADR-009)
 * - 알림: 충전소 상세에서 알림을 켜면, 그 충전소에 자리가 날 때 같은 실시간 연결로 notification을 받음 (ADR-010).
 *   로그인 대신 이 브라우저에 저장한 무작위 값(clientKey)으로 구독자를 구분함
 * 서버가 준 값은 모두 textContent로 넣어 HTML로 해석되지 않게 한다.
 */
(() => {
  'use strict';

  const DEFAULT_CENTER = { lat: 37.4979, lng: 127.0276 }; // 강남역
  const MAX_RADIUS = 10_000;
  const MIN_RADIUS = 100;
  const PAGE_SIZE = 100;
  const PERIODIC_REFRESH_MS = 120_000; // 원본 상태 수집 주기(2분)와 맞춤
  const VACANCY_REFRESH_DELAY_MS = 800;
  const SDK_TIMEOUT_MS = 8_000;
  const STREAM_RETRY_BASE_MS = 30_000; // 서버가 거절했을 때(503 등) 다시 시도 간격. Retry-After와 맞춤
  const STREAM_RETRY_JITTER_MS = 10_000; // 여러 화면이 한꺼번에 다시 붙지 않도록
  const SEEN_EVENTS_MAX = 200;
  const RESYNC_MIN_INTERVAL_MS = 10_000; // 탭을 빠르게 오가도 다시 읽기가 쌓이지 않게
  const CLIENT_KEY_STORAGE = 'ev-charging.client-key';
  const INBOX_SIZE = 20;
  const TOAST_MS = 6_000; // 일반 안내는 잠시 보였다가 사라진다
  const MAX_CARDS = 5; // 닫을 때까지 남는 알림 카드
  const MAX_PLAIN_TOASTS = 3; // 잠시 뜨는 안내
  const SWIPE_DISMISS_PX = 80; // 이만큼 옆으로 밀면 알림을 닫는다
  const MOBILE_QUERY = '(max-width: 760px)';
  const SHEET_ORDER = ['peek', 'half', 'full']; // 모바일 목록 시트: 접힘 → 절반 → 펼침
  // 손잡이를 누를 때 다음 높이. 펼침에서는 맨 아래로 떨어지지 않게 절반으로 돌아간다 (접힘은 끌거나 방향키로)
  const SHEET_TAP_NEXT = { peek: 'half', half: 'full', full: 'half' };
  const SHEET_LABELS = { peek: '목록 펼치기 (접힘)', half: '목록 크게 보기 (절반)', full: '목록 줄이기 (펼침)' };
  const SHEET_PEEK_BASE = 158; // CSS의 --sheet-peek-base와 같게
  const SWIPE_START_PX = 6; // 이보다 적게 움직이면 미는 것이 아니라 누른 것으로 본다
  const SWIPE_CLICK_GUARD_MS = 400; // 밀고 난 직후의 클릭(터치의 tap 판정 포함)은 카드를 열지 않는다

  const $ = (id) => document.getElementById(id);
  const el = (tag, className, text) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined && text !== null) node.textContent = text;
    return node;
  };

  const isMobile = () => window.matchMedia(MOBILE_QUERY).matches;

  const state = {
    sheetRest: 'half', // 상세·알림함을 닫으면 돌아갈 시트 높이
    sheetOverlay: false, // 상세·알림함 때문에 시트를 펼쳐 둔 상태인지
    zoomControl: null,
    refitUntil: 0, // 이 시각까지 지도 크기가 바뀌면 키워드 결과에 다시 맞춘다
    map: null,
    mode: 'area', // 'area' | 'keyword'
    keyword: '',
    results: [],
    overlays: new Map(), // stationId -> { overlay, node }
    selectedId: null,
    returnFocusTo: null, // 상세를 닫을 때 포커스를 돌려줄 곳 ('list' | 'pin' | 'inbox')
    focusDetailPending: false, // 상세가 그려지면 제목으로 포커스를 옮길지
    pendingFit: false, // 다음 검색 결과에 맞춰 지도를 옮길지 (키워드 검색)
    lastSyncAt: 0,
    searchSeq: 0,
    detailSeq: 0,
    refreshTimer: null,
    pendingVacancies: new Map(), // stationId -> 마지막으로 빈자리가 난 chargerId
    idleTimer: null,
    stream: null,
    streamConnectedOnce: false,
    streamRetryTimer: null,
    seenEventIds: [],
    seenNotificationIds: [],
    clientKey: null,
    subscriptions: new Map(), // stationId -> 구독 정보
    notifications: [], // 최근 알림 (최신 순)
    unread: 0,
    detail: null, // 상세에 열려 있는 충전소
    subscribeBusy: false,
    subscriptionSeq: 0, // 늦게 도착한 구독 목록 응답이 최신 상태를 덮지 않게
    cardEventIds: [], // 구독 알림 카드를 띄운 빈자리 (같은 빈자리로 두 번 띄우지 않게)
    plainToasts: new Map(), // eventId -> 빈자리 일반 토스트 (구독 알림이 오면 카드로 바꾼다)
    focusChargerId: null, // 알림으로 연 상세에서 강조할 충전기
    devTools: false, // 서버의 개발 도구가 켜져 있으면 테스트 버튼을 보여 준다
    testBusy: false,
  };

  // ---------- 서버 호출 ----------

  async function requestJson(url, { method = 'GET', body, auth = false, headers: extraHeaders = {} } = {}) {
    const headers = { Accept: 'application/json', ...extraHeaders };
    if (body !== undefined) headers['Content-Type'] = 'application/json';
    if (auth) headers['X-Client-Key'] = clientKey();
    let response;
    try {
      response = await fetch(url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
    } catch {
      throw new Error('서버에 연결할 수 없습니다. 잠시 뒤 다시 시도해 주세요.');
    }
    let result = null;
    try {
      if (response.status !== 204) result = await response.json();
    } catch {
      // JSON이 아닌 오류 응답
    }
    if (!response.ok) {
      throw new Error(result?.detail || `요청을 처리하지 못했습니다 (HTTP ${response.status})`);
    }
    return result;
  }

  const getJson = (url) => requestJson(url);

  /** 이 브라우저의 구독자 식별값. 처음 한 번 만들어 저장해 두고, 저장할 수 없으면 이 화면을 연 동안만 쓴다 */
  function clientKey() {
    if (state.clientKey) return state.clientKey;
    let key = null;
    try { key = localStorage.getItem(CLIENT_KEY_STORAGE); } catch { /* 저장소를 쓸 수 없음 */ }
    if (!key || !/^[A-Za-z0-9-]{16,64}$/.test(key)) {
      key = crypto.randomUUID ? crypto.randomUUID()
        : Array.from(crypto.getRandomValues(new Uint8Array(16)), (b) => b.toString(16).padStart(2, '0')).join('');
      try { localStorage.setItem(CLIENT_KEY_STORAGE, key); } catch { /* 이번 방문 동안만 사용 */ }
    }
    state.clientKey = key;
    return key;
  }

  // ---------- 표시 도우미 ----------

  function availabilityOf(station) {
    if (station.availableCount > 0) return 'available';
    if (station.chargingCount > 0) return 'busy';
    return 'off';
  }

  function formatDistance(meters) {
    if (meters === null || meters === undefined) return null;
    return meters < 1000 ? `${meters}m` : `${(meters / 1000).toFixed(1)}km`;
  }

  function relativeTime(iso) {
    if (!iso) return '시각 정보 없음';
    const seconds = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
    if (seconds < 60) return '방금 전';
    const minutes = Math.round(seconds / 60);
    if (minutes < 60) return `${minutes}분 전`;
    const hours = Math.round(minutes / 60);
    if (hours < 24) return `${hours}시간 전`;
    return `${Math.round(hours / 24)}일 전`;
  }

  function clock() {
    return new Date().toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }

  /**
   * 화면 오른쪽 위 알림.
   * 일반 안내는 6초 뒤 사라지고, 빈자리 알림(notify)은 휴대폰 알림처럼 옆으로 밀거나 ×를 눌러 닫을 때까지 남는다.
   * 빈자리 알림에 onOpen을 주면 카드 자체를 눌러 연다.
   */
  function toast(message, { error = false, notify = false, action, onOpen } = {}) {
    const node = el('div', `toast${error ? ' toast--error' : ''}${notify ? ' toast--notify' : ''}`);
    if (notify && onOpen) {
      const open = el('button', 'toast__open');
      open.type = 'button';
      open.append(el('span', 'toast__message', message), el('span', 'toast__hint', '눌러서 충전기 보기 · 옆으로 밀어서 닫기'));
      open.addEventListener('click', () => { onOpen(); dismissToast(node); });
      node.append(open);
    } else {
      node.append(el('div', 'toast__message', message));
    }
    if (action) {
      const button = el('button', 'toast__action', action.label);
      button.type = 'button';
      button.addEventListener('click', () => { action.run(); node.remove(); });
      node.append(button);
    }
    if (notify) {
      node.dataset.sticky = 'true';
      const close = el('button', 'toast__close', '×');
      close.type = 'button';
      close.setAttribute('aria-label', '알림 닫기');
      close.addEventListener('click', () => dismissToast(node));
      node.append(close);
      enableSwipeToDismiss(node);
    } else {
      setTimeout(() => node.remove(), TOAST_MS);
    }
    $('toasts').prepend(node);
    trimToasts();
    return node;
  }

  /**
   * 알림 카드와 일반 안내는 따로 상한을 둔다. 잠시 뜨는 안내가 남아 있어야 할 알림 카드를 밀어내지 않고,
   * 카드가 가득 차도 새 안내·오류는 보인다. 새 것은 맨 위(앞)에 붙으므로 넘친 만큼 뒤(오래된 것)부터 지운다
   */
  function trimToasts() {
    const live = [...$('toasts').children].filter((n) => !n.dataset.leaving);
    const cards = live.filter((n) => n.dataset.sticky === 'true');
    const plain = live.filter((n) => n.dataset.sticky !== 'true');
    const mobile = isMobile(); // 좁은 화면에서는 지도를 덜 가리도록 적게 쌓는다
    cards.slice(mobile ? 1 : MAX_CARDS).forEach((n) => n.remove());
    plain.slice(mobile ? 1 : MAX_PLAIN_TOASTS).forEach((n) => n.remove());
  }

  function dismissToast(node, direction = 1) {
    if (node.dataset.leaving) return;
    node.dataset.leaving = 'true';
    const reduceMotion = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;
    if (reduceMotion) { node.remove(); return; }
    node.style.transition = 'transform 0.2s ease-in, opacity 0.2s ease-in';
    node.style.transform = `translateX(${direction * 120}%)`;
    node.style.opacity = '0';
    node.addEventListener('transitionend', () => node.remove(), { once: true });
    setTimeout(() => node.remove(), 400); // transitionend가 오지 않는 경우 대비
  }

  /**
   * 옆으로 밀어서 닫기 (터치와 마우스 모두). 덜 밀면 제자리로 돌아온다.
   * 조금 움직였을 때부터 미는 것으로 보므로, 그냥 누르면 카드를 연다. 밀고 난 뒤의 클릭은 무시한다.
   */
  function enableSwipeToDismiss(node) {
    let startX = null;
    let dx = 0;
    let swiping = false;
    let suppressClickUntil = 0;
    const reset = () => { startX = null; swiping = false; };
    node.addEventListener('pointerdown', (e) => {
      if (e.target.closest('.toast__close') || (e.pointerType === 'mouse' && e.button !== 0)) return;
      startX = e.clientX;
      dx = 0;
      swiping = false;
    });
    // 마우스는 밀기 전에 카드를 벗어나 놓으면 pointerup이 오지 않는다. 그 상태가 남지 않게 정리한다
    node.addEventListener('pointerleave', () => { if (!swiping) reset(); });
    node.addEventListener('pointermove', (e) => {
      if (startX === null) return;
      if (e.pointerType === 'mouse' && e.buttons === 0) { reset(); return; }
      dx = e.clientX - startX;
      if (!swiping) {
        if (Math.abs(dx) < SWIPE_START_PX) return;
        swiping = true;
        node.style.transition = 'none';
        node.setPointerCapture(e.pointerId);
      }
      node.style.transform = `translateX(${dx}px)`;
      node.style.opacity = String(Math.max(0.2, 1 - Math.abs(dx) / 240));
    });
    node.addEventListener('click', (e) => {
      if (Date.now() >= suppressClickUntil) return;
      e.stopPropagation();
      e.preventDefault();
    }, true);
    const end = () => {
      if (startX === null) return;
      startX = null;
      if (!swiping) return; // 그냥 누른 것: 클릭으로 처리된다
      swiping = false;
      suppressClickUntil = Date.now() + SWIPE_CLICK_GUARD_MS;
      if (Math.abs(dx) >= SWIPE_DISMISS_PX) {
        dismissToast(node, Math.sign(dx));
      } else {
        node.style.transition = 'transform 0.15s ease-out, opacity 0.15s ease-out';
        node.style.transform = '';
        node.style.opacity = '';
      }
    };
    node.addEventListener('pointerup', end);
    node.addEventListener('pointercancel', end);
  }

  function setNotice(message) {
    $('map-notice').hidden = !message;
    $('map-notice').textContent = message || '';
  }

  // ---------- 검색 ----------

  function filters() {
    const params = new URLSearchParams({ size: String(PAGE_SIZE) });
    if ($('available-only').checked) params.set('availableOnly', 'true');
    const minOutput = $('min-output').value;
    if (minOutput) params.set('minOutputKw', minOutput);
    return params;
  }

  function areaQuery() {
    const params = filters();
    let center = DEFAULT_CENTER;
    let radius = 1000;
    if (state.map) {
      const c = state.map.getCenter();
      const ne = state.map.getBounds().getNorthEast();
      center = { lat: c.getLat(), lng: c.getLng() };
      radius = Math.round(haversine(center, { lat: ne.getLat(), lng: ne.getLng() }));
    }
    setNotice(radius > MAX_RADIUS ? '지도를 확대하면 보이는 영역 전체를 찾습니다 (지금은 중심에서 10km까지)' : '');
    params.set('lat', center.lat.toFixed(6));
    params.set('lng', center.lng.toFixed(6));
    params.set('radius', String(Math.min(MAX_RADIUS, Math.max(MIN_RADIUS, radius))));
    return params;
  }

  function haversine(a, b) {
    const R = 6_371_000;
    const toRad = (d) => (d * Math.PI) / 180;
    const dLat = toRad(b.lat - a.lat);
    const dLng = toRad(b.lng - a.lng);
    const h = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(a.lat)) * Math.cos(toRad(b.lat)) * Math.sin(dLng / 2) ** 2;
    return 2 * R * Math.asin(Math.sqrt(h));
  }

  async function search({ fit = false, quiet = false } = {}) {
    const params = state.mode === 'keyword'
      ? (() => { const p = filters(); p.set('q', state.keyword); return p; })()
      : areaQuery();
    // 지도 맞춤 요청은 상태로 들고 있다가 성공한 응답에서 처리한다. 그 사이 조용한 재조회가 끼어들어도 사라지지 않는다
    if (fit) state.pendingFit = true;
    const seq = ++state.searchSeq;
    if (!quiet) $('summary').textContent = '찾는 중…';
    try {
      const result = await getJson(`/api/stations?${params}`);
      if (seq !== state.searchSeq) return; // 늦게 도착한 이전 요청은 버림
      state.results = result.items;
      state.lastSyncAt = Date.now();
      renderList(result);
      renderMarkers();
      if (state.pendingFit && state.mode === 'keyword') {
        fitToResults();
        state.refitUntil = Date.now() + 1000; // 이어서 모바일 시트가 커지거나 줄어도 결과가 화면에 들어오게
      }
      state.pendingFit = false;
    } catch (e) {
      if (seq !== state.searchSeq) return;
      $('summary').textContent = '검색하지 못했습니다.';
      toast(e.message, { error: true });
    }
  }

  function renderList(result) {
    const list = $('list');
    // 다시 그려도 키보드 포커스가 사라지지 않도록, 포커스가 있던 충전소를 기억했다가 돌려준다
    const focusedId = list.contains(document.activeElement) ? document.activeElement.dataset.stationId : null;
    list.replaceChildren();

    const availableStations = result.items.filter((s) => s.availableCount > 0).length;
    const total = result.totalCount.toLocaleString('ko-KR');
    if (state.mode === 'keyword') {
      $('summary').textContent = result.totalCount > result.items.length
        ? `"${state.keyword}" 검색 ${total}곳 중 이름순 ${result.items.length}곳 · ${clock()} 갱신`
        : `"${state.keyword}" 검색 ${total}곳 · 빈자리 ${availableStations}곳 · ${clock()} 갱신`;
    } else {
      $('summary').textContent = `지도 영역 ${total}곳 · 빈자리 ${availableStations}곳 · ${clock()} 갱신`
        + (result.totalCount > result.items.length ? ` (가까운 ${result.items.length}곳만 표시)` : '');
    }

    if (result.items.length === 0) {
      list.append(el('li', 'empty', state.mode === 'keyword'
        ? '찾는 충전소가 없습니다. 다른 이름이나 주소로 검색해 보세요.'
        : '이 영역에는 조건에 맞는 충전소가 없습니다. 지도를 옮기거나 필터를 바꿔 보세요.'));
      return;
    }

    for (const station of result.items) {
      const item = el('li', 'list__item');
      item.tabIndex = 0;
      item.dataset.stationId = station.stationId;
      if (station.stationId === state.selectedId) item.setAttribute('aria-current', 'true');

      const top = el('div', 'list__top');
      top.append(el('span', 'list__name', station.name));
      const kind = availabilityOf(station);
      top.append(el('span', `badge badge--${kind}`, kind === 'off' ? '사용 불가' : `${station.availableCount}/${station.chargerCount}`));

      const meta = [
        formatDistance(station.distanceMeters),
        station.maxOutputKw ? `최대 ${station.maxOutputKw}kW` : null,
        station.parkingFree === true ? '무료 주차' : null,
        station.operatorName,
      ].filter(Boolean).join(' · ');
      item.append(top, el('div', 'list__meta', meta || station.address));

      const open = () => selectStation(station.stationId, { pan: true, from: 'list' });
      item.addEventListener('click', open);
      item.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); open(); } });
      list.append(item);
    }
    if (focusedId) listItem(focusedId)?.focus();
  }

  function listItem(stationId) {
    return document.querySelector(`.list__item[data-station-id="${CSS.escape(stationId)}"]`);
  }

  // ---------- 지도 ----------

  function renderMarkers() {
    if (!state.map) return;
    const seen = new Set();
    for (const station of state.results) {
      seen.add(station.stationId);
      const kind = availabilityOf(station);
      let entry = state.overlays.get(station.stationId);
      if (!entry) {
        const node = el('div', 'pin');
        node.setAttribute('role', 'button');
        node.tabIndex = 0;
        const open = () => selectStation(station.stationId, { pan: false, from: 'pin' });
        node.addEventListener('click', open);
        node.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); open(); } });
        node.addEventListener('animationend', () => node.classList.remove('pin--pulse'));
        const overlay = new kakao.maps.CustomOverlay({
          position: new kakao.maps.LatLng(station.latitude, station.longitude),
          content: node,
          yAnchor: 1,
        });
        overlay.setMap(state.map);
        entry = { overlay, node };
        state.overlays.set(station.stationId, entry);
      }
      // 깜박임(pin--pulse)은 남겨 두고 필요한 클래스만 바꾼다
      entry.node.classList.remove('pin--available', 'pin--busy', 'pin--off');
      entry.node.classList.add(`pin--${kind}`);
      entry.node.classList.toggle('pin--selected', station.stationId === state.selectedId);
      entry.node.textContent = `${station.availableCount}/${station.chargerCount}`;
      entry.node.setAttribute('aria-label', `${station.name}, 사용 가능 ${station.availableCount}대, 전체 ${station.chargerCount}대`);
      entry.overlay.setZIndex(station.stationId === state.selectedId ? 10 : kind === 'available' ? 2 : 1);
    }
    for (const [id, entry] of state.overlays) {
      if (!seen.has(id) && id !== state.selectedId) {
        entry.overlay.setMap(null);
        state.overlays.delete(id);
      }
    }
  }

  function fitToResults() {
    if (!state.map || state.results.length === 0) return;
    const bounds = new kakao.maps.LatLngBounds();
    state.results.forEach((s) => bounds.extend(new kakao.maps.LatLng(s.latitude, s.longitude)));
    state.map.setBounds(bounds, 40, 40, 40, 40);
  }

  function onMapIdle() {
    if (state.mode !== 'area') return; // 키워드 검색 중에는 지도를 움직여도 다시 찾지 않음
    clearTimeout(state.idleTimer);
    state.idleTimer = setTimeout(() => search(), 300);
  }

  // ---------- 상세 ----------

  /** @param chargerId 알림으로 열었을 때 강조할 충전기 */
  async function selectStation(stationId, { pan, from, chargerId = null }) {
    state.selectedId = stationId;
    state.returnFocusTo = from;
    state.focusChargerId = chargerId;
    document.querySelectorAll('.list__item').forEach((n) => {
      if (n.dataset.stationId === stationId) n.setAttribute('aria-current', 'true');
      else n.removeAttribute('aria-current');
    });
    renderMarkers();
    const station = state.results.find((s) => s.stationId === stationId);
    if (pan && station && state.map) {
      state.map.panTo(new kakao.maps.LatLng(station.latitude, station.longitude));
    }
    // 포커스 이동은 상태로 들고 있다가 상세가 실제로 그려질 때 처리한다 (다른 갱신이 이 요청을 대신해도 빠지지 않게)
    state.focusDetailPending = true;
    await loadDetail(stationId, { flashChargerId: chargerId, scrollToCharger: !!chargerId });
  }

  async function loadDetail(stationId, { flashChargerId, scrollToCharger = false } = {}) {
    const seq = ++state.detailSeq;
    try {
      const detail = await getJson(`/api/stations/${encodeURIComponent(stationId)}`);
      if (seq !== state.detailSeq || state.selectedId !== stationId) return;
      renderDetail(detail, flashChargerId);
      if (state.focusDetailPending) {
        state.focusDetailPending = false;
        // 제목으로 포커스를 옮기면 기본으로 위로 스크롤되므로, 알림 받은 충전기로 스크롤하는 경우는 스크롤 없이 옮긴다
        $('detail-name').focus({ preventScroll: scrollToCharger });
      }
      if (scrollToCharger) document.querySelector('.charger--focus')?.scrollIntoView({ block: 'nearest' });
    } catch (e) {
      if (seq !== state.detailSeq) return;
      toast(e.message, { error: true });
    }
  }

  function renderDetail(detail, flashChargerId) {
    const opening = $('detail').hidden; // 자동 갱신으로 다시 그릴 때는 사용자가 정한 시트 높이를 유지한다
    if (!$('inbox').hidden) closeInbox({ restoreFocus: false, cancelPending: false });
    if (opening) expandSheetForOverlay(state.returnFocusTo === 'pin' ? 'half' : 'full');
    state.detail = detail;
    renderSubscribeButton();
    $('detail-test').hidden = !state.devTools;
    $('detail-close').textContent = state.returnFocusTo === 'inbox' ? '← 내 알림으로' : '← 목록으로';
    $('detail').hidden = false;
    // 상세가 목록을 덮는 동안 가려진 목록·검색 영역으로 Tab이 가지 않게 한다
    $('list').inert = true;
    document.querySelector('.panel__header').inert = true;
    $('detail-name').textContent = detail.name;
    $('detail-address').textContent = [detail.address, detail.locationDetail].filter(Boolean).join(' ');

    const info = $('detail-info');
    info.replaceChildren();
    const rows = [
      ['사용 가능', `${detail.availableCount} / ${detail.chargers.length}대`],
      ['이용 시간', detail.useTime],
      ['주차', detail.parkingFree === true ? '무료' : detail.parkingFree === false ? '유료' : null],
      ['이용 제한', detail.limited ? (detail.limitDetail || '있음') : null],
      ['운영', [detail.operatorName, detail.operatorCall].filter(Boolean).join(' · ')],
    ];
    for (const [label, value] of rows) {
      if (!value) continue;
      info.append(el('dt', null, label), el('dd', null, value));
    }

    const list = $('detail-chargers');
    list.replaceChildren();
    for (const charger of detail.chargers) {
      const focused = charger.chargerId === state.focusChargerId;
      const item = el('li', `charger${charger.chargerId === flashChargerId ? ' charger--flash' : ''}${focused ? ' charger--focus' : ''}`);
      const left = el('div');
      left.append(el('div', null, `${charger.chargerId}번`));
      if (focused) {
        item.setAttribute('aria-current', 'true');
        left.append(el('div', 'charger__badge', '🔔 알림 받은 충전기'));
      }
      left.append(el('div', 'charger__sub', charger.outputKw ? `${charger.outputKw}kW` : '출력 정보 없음'));
      const statusClass = charger.status === 'AVAILABLE' || charger.status === 'CHARGING' ? charger.status : 'other';
      const right = el('div', `status status--${statusClass}`);
      right.append(el('div', null, charger.statusDescription));
      right.append(el('div', 'charger__sub', relativeTime(charger.statusUpdatedAt)));
      item.append(left, right);
      list.append(item);
    }
  }

  function closeDetail({ restoreFocus = true } = {}) {
    const closedId = state.selectedId;
    const wasOpen = !$('detail').hidden;
    state.focusDetailPending = false;
    $('detail').hidden = true;
    $('list').inert = false;
    document.querySelector('.panel__header').inert = false;
    state.selectedId = null;
    state.detail = null;
    state.focusChargerId = null;
    document.querySelectorAll('.list__item[aria-current]').forEach((n) => n.removeAttribute('aria-current'));
    renderMarkers();
    restoreSheetAfterOverlay();
    // 상세를 열었던 곳(목록 항목이나 지도 핀)으로 포커스를 돌려준다
    // 그 사이 결과에서 빠졌으면 목록 첫 항목이나 검색창으로 보낸다
    if (restoreFocus && wasOpen && closedId && state.returnFocusTo === 'inbox') {
      openInbox(); // 알림함에서 연 상세를 닫으면 알림함으로 돌아간다
      return;
    }
    if (restoreFocus && wasOpen && closedId) {
      const origin = state.returnFocusTo === 'pin' ? state.overlays.get(closedId)?.node : listItem(closedId);
      (origin ?? document.querySelector('.list__item') ?? $('keyword')).focus();
    }
  }

  // ---------- 빈자리 알림 구독 ----------

  function renderSubscribeButton() {
    const button = $('detail-subscribe');
    const subscribed = !!state.detail && state.subscriptions.has(state.detail.stationId);
    button.setAttribute('aria-pressed', String(subscribed));
    button.textContent = subscribed ? '알림 받는 중 · 끄기' : '빈자리 알림 받기';
    // disabled로 바꾸면 키보드 포커스가 빠지므로 aria-disabled만 표시하고 중복 클릭은 subscribeBusy로 막는다
    button.setAttribute('aria-disabled', String(state.subscribeBusy));
  }

  function applySubscriptions(items, seq) {
    if (seq !== state.subscriptionSeq) return false; // 그 사이 구독을 바꿨거나 더 새 요청이 있음
    state.subscriptions = new Map(items.map((s) => [s.stationId, s]));
    return true;
  }

  async function loadSubscriptions() {
    const seq = ++state.subscriptionSeq;
    try {
      const result = await requestJson('/api/subscriptions', { auth: true });
      if (!applySubscriptions(result.items, seq)) return;
      renderSubscribeButton();
      if (!$('inbox').hidden) renderInbox();
    } catch {
      // 구독 목록을 못 읽어도 검색은 계속 쓸 수 있다. 버튼을 누르면 다시 시도된다
    }
  }

  async function toggleSubscription() {
    const detail = state.detail;
    if (!detail || state.subscribeBusy) return;
    const subscribed = state.subscriptions.has(detail.stationId);
    state.subscribeBusy = true;
    state.subscriptionSeq += 1;
    renderSubscribeButton();
    try {
      if (subscribed) {
        await unsubscribe(detail.stationId);
        toast(`${detail.name} 알림을 껐어요.`);
      } else {
        askSystemNotificationPermission();
        await requestJson('/api/subscriptions', { method: 'POST', body: { stationId: detail.stationId }, auth: true });
        state.subscriptionSeq += 1;
        state.subscriptions.set(detail.stationId, { stationId: detail.stationId, stationName: detail.name, address: detail.address });
        toast(`${detail.name}에 자리가 나면 알려 드릴게요.`);
      }
    } catch (e) {
      toast(e.message, { error: true });
    } finally {
      state.subscribeBusy = false;
      renderSubscribeButton();
      loadSubscriptions(); // 다른 탭에서 바꾼 구독까지 맞춘다
    }
  }

  /**
   * 개발용: 이 충전소의 첫 충전기를 충전중 → 충전대기로 바꿔 테스트 빈자리를 만든다.
   * 실제 수집과 같은 경로(상태 → 빈자리 감지 → 알림)를 타며, 시연용 알림은 10분 제한 없이 매번 온다
   */
  async function sendTestVacancy() {
    const detail = state.detail;
    if (!detail || state.testBusy) return;
    state.testBusy = true;
    $('detail-test').setAttribute('aria-disabled', 'true');
    try {
      const result = await requestJson(`/dev/stations/${encodeURIComponent(detail.stationId)}/test-vacancy`, {
        method: 'POST',
        headers: { 'X-Requested-With': 'fetch' },
      });
      toast(state.subscriptions.has(detail.stationId)
        ? `테스트 빈자리를 만들었어요 (${result.chargerId}번). 곧 알림이 옵니다.`
        : `테스트 빈자리를 만들었어요 (${result.chargerId}번). 알림을 받으려면 먼저 "빈자리 알림 받기"를 켜 주세요.`);
    } catch (e) {
      toast(e.message, { error: true });
    } finally {
      state.testBusy = false;
      $('detail-test').setAttribute('aria-disabled', 'false');
    }
  }

  async function unsubscribe(stationId) {
    await requestJson(`/api/subscriptions/${encodeURIComponent(stationId)}`, { method: 'DELETE', auth: true });
    state.subscriptionSeq += 1;
    state.subscriptions.delete(stationId);
  }

  /** 다른 탭을 보고 있을 때도 알 수 있도록, 처음 알림을 켤 때 브라우저 알림 권한을 묻는다 (거절해도 화면 안 알림은 동작) */
  function askSystemNotificationPermission() {
    if ('Notification' in window && Notification.permission === 'default') {
      Promise.resolve(Notification.requestPermission()).catch(() => {});
    }
  }

  // ---------- 내 알림 ----------

  async function openInbox() {
    // 열려 있거나 불러오는 중인 상세를 취소한다. 늦게 온 상세 응답이 알림함을 닫지 않게
    if (state.selectedId) closeDetail({ restoreFocus: false });
    state.detailSeq += 1;
    state.focusDetailPending = false;
    $('inbox').hidden = false;
    $('list').inert = true;
    document.querySelector('.panel__header').inert = true;
    expandSheetForOverlay();
    state.unread = 0;
    renderUnread();
    renderInbox();
    $('inbox-title').focus();
    const seq = ++state.subscriptionSeq;
    try {
      const [subscriptions, notifications] = await Promise.all([
        requestJson('/api/subscriptions', { auth: true }),
        requestJson('/api/notifications', { auth: true }),
      ]);
      applySubscriptions(subscriptions.items, seq);
      mergeNotifications(notifications.items);
      if (!$('inbox').hidden) renderInbox();
    } catch (e) {
      toast(e.message, { error: true });
    }
  }

  /** @param cancelPending 알림함에서 누른 상세를 아직 불러오는 중이면 취소할지. 늦게 온 응답이 닫은 뒤에 상세를 열지 않게 */
  function closeInbox({ restoreFocus = true, cancelPending = true } = {}) {
    if ($('inbox').hidden) return;
    if (cancelPending && $('detail').hidden && state.selectedId) {
      closeDetail({ restoreFocus: false });
      state.detailSeq += 1;
      state.focusDetailPending = false;
    }
    $('inbox').hidden = true;
    $('list').inert = false;
    document.querySelector('.panel__header').inert = false;
    restoreSheetAfterOverlay();
    if (restoreFocus) $('inbox-open').focus();
  }

  function inboxItem(title, sub, onOpen) {
    const item = el('li', 'inbox__item');
    const open = el('button', 'inbox__main');
    open.type = 'button';
    open.append(el('span', null, title), el('span', 'inbox__sub', sub));
    open.addEventListener('click', onOpen);
    item.append(open);
    return item;
  }

  function renderInbox() {
    const subscriptions = $('inbox-subscriptions');
    subscriptions.replaceChildren();
    if (state.subscriptions.size === 0) {
      subscriptions.append(el('li', 'inbox__empty', '아직 알림을 켠 충전소가 없습니다. 충전소 상세에서 "빈자리 알림 받기"를 누르세요.'));
    }
    for (const sub of state.subscriptions.values()) {
      const item = inboxItem(
        sub.stationName,
        sub.stationActive === false ? '운영 정보에서 사라진 충전소' : sub.address,
        () => selectStation(sub.stationId, { pan: true, from: 'inbox' }),
      );
      const off = el('button', 'link-button', '끄기');
      off.type = 'button';
      off.setAttribute('aria-label', `${sub.stationName} 알림 끄기`);
      off.addEventListener('click', async () => {
        try {
          await unsubscribe(sub.stationId);
          renderInbox();
          renderSubscribeButton();
          $('inbox-title').focus();
        } catch (e) {
          toast(e.message, { error: true });
        }
      });
      item.append(off);
      subscriptions.append(item);
    }

    const notifications = $('inbox-notifications');
    notifications.replaceChildren();
    if (state.notifications.length === 0) {
      notifications.append(el('li', 'inbox__empty', '받은 알림이 없습니다.'));
    }
    for (const n of state.notifications) {
      notifications.append(inboxItem(
        `${n.stationName} ${n.chargerId}번 충전기 빈자리`,
        relativeTime(n.occurredAt),
        () => selectStation(n.stationId, { pan: true, from: 'inbox', chargerId: n.chargerId }),
      ));
    }
  }

  /**
   * 서버의 알림함과 화면이 받은 알림을 합친다.
   * 발송기는 화면으로 보낸 뒤 SENT를 기록하므로, 방금 받은 알림이 서버 목록에 아직 없을 수 있다
   */
  function mergeNotifications(items) {
    const byId = new Map();
    for (const n of [...items, ...state.notifications]) if (!byId.has(n.notificationId)) byId.set(n.notificationId, n);
    const time = (n) => new Date(n.sentAt || n.occurredAt || 0).getTime();
    state.notifications = [...byId.values()].sort((a, b) => time(b) - time(a)).slice(0, INBOX_SIZE);
  }

  function renderUnread() {
    $('inbox-count').hidden = state.unread === 0;
    $('inbox-count').textContent = String(state.unread);
    $('inbox-open').setAttribute('aria-label', state.unread ? `내 알림, 새 알림 ${state.unread}건` : '내 알림');
  }

  function onNotification(n) {
    // 발송은 최소 한 번이라 같은 알림이 다시 올 수 있다
    if (state.seenNotificationIds.includes(n.notificationId)) return;
    state.seenNotificationIds.push(n.notificationId);
    if (state.seenNotificationIds.length > SEEN_EVENTS_MAX) state.seenNotificationIds.shift();

    state.notifications = [n, ...state.notifications.filter((x) => x.notificationId !== n.notificationId)].slice(0, INBOX_SIZE);
    const message = `${n.stationName} ${n.chargerId}번 충전기에 자리가 났어요`;
    if (!state.cardEventIds.includes(n.eventId)) {
      state.cardEventIds.push(n.eventId);
      if (state.cardEventIds.length > SEEN_EVENTS_MAX) state.cardEventIds.shift();
      // 같은 빈자리의 일반 토스트가 먼저 떠 있으면 알림 카드로 바꾼다
      state.plainToasts.get(n.eventId)?.remove();
      state.plainToasts.delete(n.eventId);
      toast(`🔔 ${message}`, {
        notify: true,
        onOpen: () => selectStation(n.stationId, { pan: true, from: 'list', chargerId: n.chargerId }),
      });
    }
    if ($('inbox').hidden) {
      state.unread += 1;
      renderUnread();
    } else {
      renderInbox();
    }
    if (document.hidden && 'Notification' in window && Notification.permission === 'granted') {
      try {
        const system = new Notification('EV 충전 빈자리', { body: message, tag: `notification-${n.notificationId}` });
        system.onclick = () => {
          window.focus();
          selectStation(n.stationId, { pan: true, from: 'list', chargerId: n.chargerId });
          system.close();
        };
      } catch {
        // 일부 브라우저(모바일)는 페이지에서 직접 알림을 만들 수 없다
      }
    }
  }

  // ---------- 실시간 (SSE) ----------

  function connectStream() {
    if (!('EventSource' in window)) {
      setLive('closed', '실시간 미지원');
      return;
    }
    clearTimeout(state.streamRetryTimer);
    // EventSource는 헤더를 붙일 수 없어 구독자 식별값을 쿼리로 보낸다
    const source = new EventSource(`/api/vacancies/stream?clientKey=${encodeURIComponent(clientKey())}`);
    state.stream = source;
    source.addEventListener('connected', () => {
      setLive('open', '실시간');
      // 다시 연결된 경우 끊긴 동안 놓친 빈자리를 맞추려고 바로 다시 읽는다
      if (state.streamConnectedOnce) resync();
      state.streamConnectedOnce = true;
    });
    source.addEventListener('vacancy', (e) => {
      let push;
      try { push = JSON.parse(e.data); } catch { return; }
      onVacancy(push);
    });
    source.addEventListener('notification', (e) => {
      let notification;
      try { notification = JSON.parse(e.data); } catch { return; }
      onNotification(notification);
    });
    source.onerror = () => {
      if (source.readyState === EventSource.CLOSED) {
        // 서버가 거절(503 등)하거나 HTTP 오류를 주면 EventSource는 스스로 다시 붙지 않는다. 잠시 뒤 직접 다시 연결한다
        source.close();
        const delay = STREAM_RETRY_BASE_MS + Math.round(Math.random() * STREAM_RETRY_JITTER_MS);
        setLive('closed', `${Math.round(delay / 1000)}초 뒤 다시 연결`);
        state.streamRetryTimer = setTimeout(connectStream, delay);
      } else {
        setLive('connecting', '다시 연결 중'); // 네트워크 끊김: EventSource가 스스로 다시 연결한다
      }
    };
  }

  function setLive(stateName, label) {
    $('live').dataset.state = stateName;
    $('live').textContent = label;
  }

  function resync({ force = false } = {}) {
    if (!force && Date.now() - state.lastSyncAt < RESYNC_MIN_INTERVAL_MS) return;
    state.lastSyncAt = Date.now();
    search({ quiet: true });
    if (state.selectedId) loadDetail(state.selectedId);
  }

  function alreadySeen(eventId) {
    // 빈자리 이벤트는 최소 한 번 전달되므로 같은 이벤트가 다시 올 수 있다
    if (state.seenEventIds.includes(eventId)) return true;
    state.seenEventIds.push(eventId);
    if (state.seenEventIds.length > SEEN_EVENTS_MAX) state.seenEventIds.shift();
    return false;
  }

  function onVacancy(push) {
    const station = state.results.find((s) => s.stationId === push.stationId);
    if (!station && state.selectedId !== push.stationId) return; // 화면에 없는 충전소
    if (alreadySeen(push.eventId)) return;

    // 화면에 보이는 충전소의 빈자리는 잠시 보였다 사라지는 일반 토스트로 알린다.
    // 닫을 때까지 남는 카드는 서버가 실제로 보낸 구독 알림(notification)만 띄운다(쿨다운 적용).
    // 구독 알림이 먼저 와서 카드를 띄웠으면 일반 토스트는 생략한다
    if (!state.cardEventIds.includes(push.eventId)) {
      const name = station ? station.name : $('detail-name').textContent || '선택한 충전소';
      const demo = push.source === 'MANUAL' ? ' (시연)' : '';
      const openCharger = () => selectStation(push.stationId, { pan: true, from: 'list', chargerId: push.chargerId });
      const node = toast(`${name} ${push.chargerId}번 충전기가 비었어요${demo}`, {
        action: state.selectedId === push.stationId ? null : { label: '보기', run: openCharger },
      });
      state.plainToasts.set(push.eventId, node);
      setTimeout(() => state.plainToasts.delete(push.eventId), TOAST_MS);
    }

    const entry = state.overlays.get(push.stationId);
    if (entry) {
      entry.node.classList.remove('pin--pulse');
      void entry.node.offsetWidth; // 애니메이션 다시 시작
      entry.node.classList.add('pin--pulse');
    }
    listItem(push.stationId)?.classList.add('list__item--flash');

    // 짧은 시간에 여러 빈자리가 와도 한 번에 다시 읽되, 선택한 충전소의 상세 갱신은 빠뜨리지 않는다
    state.pendingVacancies.set(push.stationId, push.chargerId);
    clearTimeout(state.refreshTimer);
    state.refreshTimer = setTimeout(() => {
      const pending = state.pendingVacancies;
      state.pendingVacancies = new Map();
      search({ quiet: true });
      if (state.selectedId && pending.has(state.selectedId)) {
        loadDetail(state.selectedId, { flashChargerId: pending.get(state.selectedId) });
      }
    }, VACANCY_REFRESH_DELAY_MS);
  }

  // ---------- 시작 ----------

  function loadKakaoSdk(key) {
    const load = new Promise((resolve, reject) => {
      const script = document.createElement('script');
      script.src = `https://dapi.kakao.com/v2/maps/sdk.js?appkey=${encodeURIComponent(key)}&autoload=false`;
      script.onload = () => (window.kakao?.maps ? kakao.maps.load(resolve) : reject(new Error('sdk')));
      script.onerror = () => reject(new Error('sdk'));
      document.head.append(script);
    });
    const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), SDK_TIMEOUT_MS));
    return Promise.race([load, timeout]);
  }

  function showMapMessage(message) {
    $('map-message').hidden = false;
    $('map-message').textContent = message;
  }

  // ---------- 모바일 목록 시트 ----------

  function setSheet(next, { rest = true } = {}) {
    const app = document.querySelector('.app');
    if (rest) state.sheetRest = next;
    $('sheet-handle').setAttribute('aria-label', SHEET_LABELS[next]);
    if (app.dataset.sheet === next) return;
    app.dataset.sheet = next;
  }

  /**
   * 상세·알림함을 열면 내용이 잘 보이게 시트를 키우고, 닫으면 원래 높이로 돌아간다.
   * 지도 핀으로 연 상세는 그 핀이 보이도록 절반까지만 키운다
   */
  function expandSheetForOverlay(level = 'full') {
    if (!isMobile()) return;
    const current = document.querySelector('.app').dataset.sheet;
    state.sheetOverlay = true;
    if (SHEET_ORDER.indexOf(level) > SHEET_ORDER.indexOf(current)) setSheet(level, { rest: false });
  }

  function restoreSheetAfterOverlay() {
    // 상세를 닫으며 곧바로 알림함을 여는 경우처럼, 이어서 다른 덮개가 열리면 키운 채로 둔다
    setTimeout(() => {
      if (!state.sheetOverlay || !$('detail').hidden || !$('inbox').hidden) return;
      state.sheetOverlay = false;
      setSheet(state.sheetRest, { rest: false });
    }, 0);
  }

  /** 사용자가 손잡이로 높이를 정하면, 덮개를 닫아도 그 높이를 유지한다 */
  function chooseSheet(next) {
    state.sheetOverlay = false;
    setSheet(next);
  }

  /**
   * 지도 영역의 크기가 바뀔 때마다(시트를 끄는 중, 전환 애니메이션, 동작 줄이기로 즉시 바뀔 때, 화면 회전) 지도 크기를 다시 계산한다.
   * 키워드 검색 직후라면 결과에 다시 맞춘다
   */
  function watchMapSize() {
    if (!('ResizeObserver' in window)) return;
    let frame = 0;
    new ResizeObserver(() => {
      cancelAnimationFrame(frame);
      frame = requestAnimationFrame(() => {
        if (!state.map) return;
        state.map.relayout();
        if (Date.now() < state.refitUntil) fitToResults();
      });
    }).observe($('map'));
  }

  /** 손잡이: 누르면 접힘 → 절반 → 펼침 → 절반 순으로, 끌면 손을 뗀 위치에서 가장 가까운 높이에 붙는다. 방향키로도 바꾼다 */
  function bindSheetHandle() {
    const panel = $('panel');
    const handle = $('sheet-handle');
    let startY = null;
    let startHeight = 0;
    let moved = false;
    let suppressClickUntil = 0;
    const appHeight = () => document.querySelector('.app').getBoundingClientRect().height;
    const reset = () => {
      startY = null;
      moved = false;
      panel.classList.remove('is-dragging');
      panel.style.height = '';
    };
    handle.addEventListener('pointerdown', (e) => {
      if (!isMobile() || !e.isPrimary || e.button !== 0) return;
      startY = e.clientY;
      startHeight = panel.getBoundingClientRect().height;
      moved = false;
      suppressClickUntil = 0;
      handle.setPointerCapture(e.pointerId);
    });
    handle.addEventListener('pointermove', (e) => {
      if (startY === null || !e.isPrimary) return;
      if (e.pointerType === 'mouse' && e.buttons === 0) { reset(); return; } // 놓친 pointerup 정리
      const dy = e.clientY - startY;
      if (!moved && Math.abs(dy) < 6) return;
      moved = true;
      panel.classList.add('is-dragging');
      panel.style.height = `${Math.min(appHeight() * 0.92, Math.max(120, startHeight - dy))}px`;
    });
    const end = () => {
      if (startY === null) return;
      const wasMoved = moved;
      const height = panel.getBoundingClientRect().height;
      reset();
      if (!wasMoved) return; // 누르기만 한 경우는 click에서 처리
      // 접힘 높이 = 기본 높이 + 홈 바 여백(패널 아래쪽 padding)
      const peek = SHEET_PEEK_BASE + (parseFloat(getComputedStyle(panel).paddingBottom) || 0);
      const targets = { peek, half: appHeight() * 0.52, full: appHeight() * 0.88 };
      const nearest = Object.entries(targets).sort((a, b) => Math.abs(a[1] - height) - Math.abs(b[1] - height))[0][0];
      chooseSheet(nearest);
      suppressClickUntil = Date.now() + 400; // 끌고 난 직후 따라오는 click만 무시 (따라오지 않아도 다음 탭은 정상)
    };
    handle.addEventListener('pointerup', end);
    handle.addEventListener('pointercancel', end);
    handle.addEventListener('lostpointercapture', end);
    handle.addEventListener('click', (e) => {
      // 키보드로 누른 click은 detail이 0이라 막지 않는다
      if (e.detail !== 0 && Date.now() < suppressClickUntil) return;
      const current = document.querySelector('.app').dataset.sheet;
      chooseSheet(SHEET_TAP_NEXT[current] ?? 'half');
    });
    handle.addEventListener('keydown', (e) => {
      const index = SHEET_ORDER.indexOf(document.querySelector('.app').dataset.sheet);
      const next = { ArrowUp: index + 1, ArrowDown: index - 1, Home: 0, End: SHEET_ORDER.length - 1 }[e.key];
      if (next === undefined) return;
      e.preventDefault();
      chooseSheet(SHEET_ORDER[Math.max(0, Math.min(SHEET_ORDER.length - 1, next))]);
    });
    // 화면 폭이 바뀌어 모바일과 데스크톱을 오가면: 끄던 상태를 지우고, 확대 버튼과 쌓인 알림을 맞춘다
    // (일부 환경은 미디어 쿼리 change를 보내지 않으므로 resize도 함께 본다)
    let wasMobile = isMobile();
    const onBreakpoint = () => {
      const now = isMobile();
      if (now === wasMobile) return;
      wasMobile = now;
      reset();
      updateZoomControl();
      trimToasts();
    };
    window.matchMedia(MOBILE_QUERY).addEventListener('change', onBreakpoint);
    window.addEventListener('resize', onBreakpoint);
  }

  /** 휴대폰은 두 손가락으로 확대하므로 확대 버튼을 두지 않는다 (핀을 가리지 않게) */
  function updateZoomControl() {
    if (!state.map) return;
    if (!state.zoomControl) state.zoomControl = new kakao.maps.ZoomControl();
    if (isMobile()) state.map.removeControl(state.zoomControl);
    else state.map.addControl(state.zoomControl, kakao.maps.ControlPosition.RIGHT);
  }

  function initMap() {
    state.map = new kakao.maps.Map($('map'), {
      center: new kakao.maps.LatLng(DEFAULT_CENTER.lat, DEFAULT_CENTER.lng),
      level: 4,
    });
    updateZoomControl();
    kakao.maps.event.addListener(state.map, 'idle', onMapIdle);
    if ('geolocation' in navigator) $('locate').hidden = false;
  }

  function bindUi() {
    bindSheetHandle();
    watchMapSize();
    $('search-form').addEventListener('submit', (e) => {
      e.preventDefault();
      const keyword = $('keyword').value.trim();
      if (keyword.length === 0) { backToArea(); return; }
      if (keyword.length < 2) { toast('검색어는 2자 이상 입력해 주세요.', { error: true }); return; }
      state.mode = 'keyword';
      state.keyword = keyword;
      $('back-to-area').hidden = false;
      setNotice('');
      if (state.selectedId) closeDetail({ restoreFocus: false }); // 사용자가 입력 중인 검색창에서 포커스를 빼앗지 않음
      if (isMobile() && document.querySelector('.app').dataset.sheet === 'peek') setSheet('half'); // 결과가 보이게
      search({ fit: true });
    });
    $('back-to-area').addEventListener('click', () => backToArea());
    $('available-only').addEventListener('change', () => search({ fit: state.mode === 'keyword' }));
    $('min-output').addEventListener('change', () => search({ fit: state.mode === 'keyword' }));
    $('detail-close').addEventListener('click', closeDetail);
    $('detail-subscribe').addEventListener('click', toggleSubscription);
    $('detail-test').addEventListener('click', sendTestVacancy);
    $('inbox-open').addEventListener('click', openInbox);
    $('inbox-close').addEventListener('click', () => closeInbox());
    $('locate').addEventListener('click', () => {
      navigator.geolocation.getCurrentPosition(
        (pos) => {
          backToArea({ searchNow: false });
          state.map.setLevel(4);
          state.map.setCenter(new kakao.maps.LatLng(pos.coords.latitude, pos.coords.longitude));
        },
        () => toast('위치를 가져오지 못했습니다. 브라우저의 위치 권한을 확인해 주세요.', { error: true }),
        { timeout: 10_000 },
      );
    });
    document.addEventListener('keydown', (e) => {
      if (e.key !== 'Escape') return;
      if (!$('detail').hidden) closeDetail();
      else if (!$('inbox').hidden) closeInbox();
    });
    // 탭이 가려진 동안에는 주기 조회를 건너뛰고, 돌아오면 바로 다시 읽는다
    document.addEventListener('visibilitychange', () => { if (!document.hidden) resync(); });
    setInterval(() => { if (!document.hidden) resync({ force: true }); }, PERIODIC_REFRESH_MS);
  }

  function backToArea({ searchNow = true } = {}) {
    state.mode = 'area';
    state.keyword = '';
    state.pendingFit = false;
    $('keyword').value = '';
    $('back-to-area').hidden = true;
    if (searchNow) search();
  }

  async function start() {
    bindUi();
    connectStream();
    loadSubscriptions();
    let key = '';
    try {
      const config = await getJson('/api/client-config');
      key = config.kakaoMapJsKey || '';
      state.devTools = config.devToolsEnabled === true;
      if (state.detail) $('detail-test').hidden = !state.devTools;
    } catch (e) {
      toast(e.message, { error: true });
    }
    if (key) {
      try {
        await loadKakaoSdk(key);
        initMap();
      } catch {
        showMapMessage('지도를 불러오지 못했습니다. 카카오 개발자 콘솔의 JavaScript SDK 도메인과 카카오맵 사용 설정을 확인해 주세요. 목록 검색은 그대로 쓸 수 있습니다.');
      }
    } else {
      showMapMessage('지도 키가 설정되지 않아 목록으로만 보여 줍니다. 강남역 주변 1km를 먼저 찾았습니다.');
    }
    search();
  }

  start();
})();
