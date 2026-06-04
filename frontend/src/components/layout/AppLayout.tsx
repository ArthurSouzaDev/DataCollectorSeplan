import { Outlet } from 'react-router-dom';

export function AppLayout() {
  return (
    <main className="app-shell">
      <Outlet />
    </main>
  );
}
