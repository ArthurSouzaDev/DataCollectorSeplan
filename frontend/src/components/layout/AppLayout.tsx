import { NavLink, Outlet } from 'react-router-dom';
import { useEffect, useRef, useState } from 'react';

import { DATASETS } from '../../services/data/transferData';

function MoonIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
      <path d="M12 3a9 9 0 1 0 9 9c0-.46-.04-.92-.1-1.36a5.389 5.389 0 0 1-4.4 2.26 5.403 5.403 0 0 1-3.14-9.8c-.44-.06-.9-.1-1.36-.1z" />
    </svg>
  );
}

function SunIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
      <path d="M12 7a5 5 0 1 0 0 10A5 5 0 0 0 12 7zm0-5a1 1 0 0 1 1 1v2a1 1 0 0 1-2 0V3a1 1 0 0 1 1-1zm0 16a1 1 0 0 1 1 1v2a1 1 0 0 1-2 0v-2a1 1 0 0 1 1-1zM4.22 5.64a1 1 0 0 1 1.42-1.42l1.41 1.41a1 1 0 0 1-1.41 1.42L4.22 5.64zm12.73 12.73a1 1 0 0 1 1.41-1.41l1.41 1.41a1 1 0 0 1-1.41 1.41l-1.41-1.41zM3 13H1a1 1 0 0 1 0-2h2a1 1 0 0 1 0 2zm20 0h-2a1 1 0 0 1 0-2h2a1 1 0 0 1 0 2zM5.64 19.78a1 1 0 0 1-1.42-1.41l1.41-1.41a1 1 0 0 1 1.42 1.41L5.64 19.78zm12.73-12.73a1 1 0 0 1-1.41-1.42l1.41-1.41a1 1 0 0 1 1.41 1.42l-1.41 1.41z" />
    </svg>
  );
}

function MenuIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" aria-hidden="true">
      <line x1="3" y1="6" x2="21" y2="6" />
      <line x1="3" y1="12" x2="21" y2="12" />
      <line x1="3" y1="18" x2="21" y2="18" />
    </svg>
  );
}

function CloseIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" aria-hidden="true">
      <line x1="5" y1="5" x2="19" y2="19" />
      <line x1="19" y1="5" x2="5" y2="19" />
    </svg>
  );
}

export function AppLayout() {
  const [theme, setTheme] = useState(() => localStorage.getItem('theme') ?? 'light');
  const [navOpen, setNavOpen] = useState(false);
  const topBarInnerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem('theme', theme);
  }, [theme]);

  useEffect(() => {
    if (!navOpen) return;

    function handleOutsideInteraction(event: PointerEvent) {
      if (!topBarInnerRef.current?.contains(event.target as Node)) {
        setNavOpen(false);
      }
    }

    document.addEventListener('pointerdown', handleOutsideInteraction);
    return () => document.removeEventListener('pointerdown', handleOutsideInteraction);
  }, [navOpen]);

  return (
    <div className="app-shell">
      <header className="top-bar">
        <div className="top-bar-inner" ref={topBarInnerRef}>
          <a className="brand" href="/">
            <img
              src="https://upload.wikimedia.org/wikipedia/commons/thumb/c/cc/Bras%C3%A3o_do_Tocantins.svg/250px-Bras%C3%A3o_do_Tocantins.svg.png"
              alt="Brasão do Tocantins"
              className="brand-seal"
            />
            <div className="brand-text">
              <span>Secretaria de Planejamento — SEPLAN</span>
              <strong>DataCollector BI</strong>
            </div>
          </a>

          <nav id="top-nav-menu" className={`top-nav${navOpen ? ' open' : ''}`} aria-label="Módulos de dados">
            {DATASETS.map((config) => (
              <NavLink
                key={config.id}
                to={`/dashboard/${config.id}`}
                className={({ isActive }) => `top-nav-link${isActive ? ' active' : ''}`}
                onClick={() => setNavOpen(false)}
              >
                {config.shortTitle}
              </NavLink>
            ))}
          </nav>

          <div className="top-bar-actions">
            <button
              className="theme-toggle"
              type="button"
              onClick={() => setTheme((current) => (current === 'dark' ? 'light' : 'dark'))}
              aria-label={theme === 'dark' ? 'Ativar modo claro' : 'Ativar modo escuro'}
            >
              {theme === 'dark' ? <SunIcon /> : <MoonIcon />}
            </button>
            <button
              className="theme-toggle nav-toggle"
              type="button"
              aria-expanded={navOpen}
              aria-controls="top-nav-menu"
              aria-label={navOpen ? 'Fechar menu de navegação' : 'Abrir menu de navegação'}
              onClick={() => setNavOpen((current) => !current)}
            >
              {navOpen ? <CloseIcon /> : <MenuIcon />}
            </button>
          </div>
        </div>
      </header>

      <main>
        <Outlet />
      </main>

      <footer className="app-footer">
        <div className="footer-inner">
          <span>© {new Date().getFullYear()} Secretaria de Planejamento do Estado do Tocantins</span>
          <span>Desenvolvido por Arthur Souza · Fonte: Transferegov · SICONV · Governo Federal</span>
        </div>
      </footer>
    </div>
  );
}
