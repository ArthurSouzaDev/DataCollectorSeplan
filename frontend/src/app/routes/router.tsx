import { lazy, Suspense } from 'react';
import { createBrowserRouter } from 'react-router-dom';

import { AppLayout } from '../../components/layout/AppLayout';
import { AjudaPage } from '../../pages/AjudaPage';
import { HistoricoPage } from '../../pages/HistoricoPage';
import { HomePage } from '../../pages/HomePage';
import { NotFoundPage } from '../../pages/NotFoundPage';

// A página de detalhe carrega o Recharts. Mantê-la em chunk separado evita que
// a home baixe a biblioteca de gráficos antes de o usuário abrir um dashboard.
const DatasetDetailPage = lazy(() =>
  import('../../pages/DatasetDetailPage').then((module) => ({ default: module.DatasetDetailPage })),
);

// O módulo completo de especiais tem muitos gráficos: também fica em chunk separado.
const TransfEspeciaisPage = lazy(() =>
  import('../../pages/TransfEspeciaisPage').then((module) => ({ default: module.TransfEspeciaisPage })),
);

export const router = createBrowserRouter([
  {
    path: '/',
    element: <AppLayout />,
    children: [
      {
        index: true,
        element: <HomePage />,
      },
      {
        path: 'dashboard/:datasetId',
        element: (
          <Suspense fallback={<div className="loading-panel">Carregando painel...</div>}>
            <DatasetDetailPage />
          </Suspense>
        ),
      },
      {
        path: 'transf-especiais',
        element: (
          <Suspense fallback={<div className="loading-panel">Carregando módulo...</div>}>
            <TransfEspeciaisPage />
          </Suspense>
        ),
      },
      {
        path: 'historico',
        element: <HistoricoPage />,
      },
      {
        path: 'ajuda',
        element: <AjudaPage />,
      },
      {
        path: '*',
        element: <NotFoundPage />,
      },
    ],
  },
]);
