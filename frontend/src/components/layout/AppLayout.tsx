import { Outlet } from 'react-router-dom';
import { useEffect, useState } from 'react';

export function AppLayout() {
  const [theme, setTheme] = useState(() => localStorage.getItem('theme') ?? 'light');

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem('theme', theme);
  }, [theme]);

  return (
    <div className="app-shell">
      <header className="top-bar">
        <a className="brand" href="/">
          <span>SEPLAN TO</span>
          <strong>DataCollector</strong>
        </a>
        <button
          className="theme-toggle"
          type="button"
          onClick={() => setTheme((current) => (current === 'dark' ? 'light' : 'dark'))}
          aria-label="Alternar dark mode"
        >
          {theme === 'dark' ? 'Modo claro' : 'Dark mode'}
        </button>
      </header>
      <main>
        <Outlet />
      </main>
    </div>
  );
}
