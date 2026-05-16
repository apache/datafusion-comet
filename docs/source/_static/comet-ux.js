/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Comet docs UX enhancements layered on top of pydata_sphinx_theme:
 *   - Click-to-toggle expand/collapse arrows on sidebar nav groups
 *   - In-sidebar search filter with character highlighting (replaces the
 *     default Sphinx redirect-to-/search.html flow)
 *   - Verbose "Comet X.Y.Z User Guide" breadcrumb labels trimmed to "X.Y.Z"
 *   - Right-side TOC auto-hidden on pages with too few headings to navigate
 *
 * All behaviour is additive — pydata's existing keyboard navigation, theme
 * switcher, search index, and toctree generation continue to work unchanged.
 */

(function () {
  'use strict';

  // ── Sidebar collapse/expand toggle buttons ─────────────────────────
  // Pydata's toctree renders a flat <ul> tree with no toggles. We inject a
  // real <button> for every nav item that has children so the user can
  // expand/collapse sections independently of which page they're on.
  function initSidebarToggle() {
    var items = document.querySelectorAll(
      '.bd-links li.toctree-l1, .bd-links li.toctree-l2'
    );
    items.forEach(function (li) {
      var ul = li.querySelector(':scope > ul');
      var link = li.querySelector(':scope > a');
      if (!ul || !link) return;
      if (li.querySelector(':scope > .comet-toggle')) return;

      // Auto-expand the section that contains the current page so the
      // active page is always visible without an extra click.
      if (li.classList.contains('current')) {
        li.classList.add('comet-expanded');
      }

      var btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'comet-toggle';
      btn.setAttribute('aria-label', 'Toggle section');
      btn.setAttribute(
        'aria-expanded',
        li.classList.contains('comet-expanded') ? 'true' : 'false'
      );
      btn.addEventListener('click', function (e) {
        e.preventDefault();
        e.stopPropagation();
        var expanded = li.classList.toggle('comet-expanded');
        btn.setAttribute('aria-expanded', expanded ? 'true' : 'false');
      });
      link.parentNode.insertBefore(btn, link.nextSibling);
    });
  }

  // ── In-sidebar search filter ────────────────────────────────────────
  // Replaces Sphinx's "submit form → land on /search.html" flow with a
  // live filter that hides non-matching nav items and highlights the
  // matched characters in-place. Search of full document content still
  // works via the regular Sphinx search page (pressing Enter is just
  // suppressed here so the form doesn't navigate away).
  function escapeRegex(s) {
    return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }
  function escapeHtml(s) {
    var map = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
    return s.replace(/[&<>"']/g, function (c) { return map[c]; });
  }

  function initSidebarSearch() {
    var input = document.querySelector(
      '.bd-sidebar form.bd-search input[type="search"], ' +
      '.bd-sidebar form.bd-search input.form-control'
    );
    var form = document.querySelector('.bd-sidebar form.bd-search');
    var nav = document.querySelector('.bd-links');
    if (!input || !nav) return;

    if (form) {
      form.addEventListener('submit', function (e) { e.preventDefault(); });
    }

    // Cache the original text so we can restore it (and re-highlight on
    // every keystroke) without losing nav item labels.
    var allLinks = Array.from(nav.querySelectorAll('a.reference'));
    allLinks.forEach(function (a) {
      if (!a.dataset.cometOrigText) {
        a.dataset.cometOrigText = a.textContent;
      }
    });
    var allListItems = Array.from(nav.querySelectorAll('li'));

    function applyFilter(query) {
      query = (query || '').trim();

      if (!query) {
        // Clear filter: restore original text, drop filter classes.
        allLinks.forEach(function (a) {
          a.innerHTML = escapeHtml(a.dataset.cometOrigText);
        });
        allListItems.forEach(function (li) {
          li.classList.remove(
            'comet-filter-hidden',
            'comet-filter-match',
            'comet-filter-expanded'
          );
        });
        nav.classList.remove('comet-filtering');
        return;
      }

      nav.classList.add('comet-filtering');
      var re = new RegExp('(' + escapeRegex(query) + ')', 'ig');

      allListItems.forEach(function (li) {
        li.classList.add('comet-filter-hidden');
        li.classList.remove('comet-filter-match', 'comet-filter-expanded');
      });

      allLinks.forEach(function (a) {
        var orig = a.dataset.cometOrigText;
        if (re.test(orig)) {
          re.lastIndex = 0;
          a.innerHTML = escapeHtml(orig).replace(
            re,
            '<mark class="comet-hl">$1</mark>'
          );
          // Reveal this item and every ancestor li so the match is reachable.
          var li = a.closest('li');
          while (li && nav.contains(li)) {
            li.classList.remove('comet-filter-hidden');
            li.classList.add('comet-filter-match', 'comet-filter-expanded');
            li = li.parentElement.closest('li');
          }
        } else {
          a.innerHTML = escapeHtml(orig);
        }
      });
    }

    var debounceTimer;
    input.addEventListener('input', function () {
      clearTimeout(debounceTimer);
      var q = input.value;
      debounceTimer = setTimeout(function () { applyFilter(q); }, 80);
    });

    input.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') {
        input.value = '';
        applyFilter('');
      } else if (e.key === 'Enter') {
        e.preventDefault();
      }
    });
  }

  // ── Breadcrumb trim ────────────────────────────────────────────────
  // "Comet 0.16.0-SNAPSHOT User Guide" reads as noise in a breadcrumb
  // when the section is already shown in the top nav. Trim to "0.16.0-
  // SNAPSHOT" and let the parent breadcrumb item carry the section name.
  function initBreadcrumbTrim() {
    document
      .querySelectorAll('.bd-breadcrumbs .breadcrumb-item a')
      .forEach(function (a) {
        var match = a.textContent
          .trim()
          .match(/^Comet\s+([\d.x\-A-Za-z]+)\s+User Guide$/);
        if (match) a.textContent = match[1];
      });
  }

  // ── Right-side TOC visibility ──────────────────────────────────────
  // Reference-style pages (config tables, expression lists) and short
  // narrative pages don't benefit from an in-page TOC. Hide the rail
  // when there are fewer than 3 anchors so it doesn't eat real estate
  // for nothing.
  function initSecondaryTocVisibility() {
    var sidebar = document.querySelector('.bd-sidebar-secondary');
    if (!sidebar) return;
    var anchors = sidebar.querySelectorAll(
      'nav.bd-toc a.reference, .page-toc a.reference'
    );
    if (anchors.length < 3) {
      sidebar.classList.add('comet-toc-empty');
    }
  }

  function bootstrap() {
    initSidebarToggle();
    initSidebarSearch();
    initSecondaryTocVisibility();
    initBreadcrumbTrim();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bootstrap);
  } else {
    bootstrap();
  }
})();
