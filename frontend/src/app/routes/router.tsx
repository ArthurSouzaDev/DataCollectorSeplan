import { lazy, Suspense } from 'react';
import { createBrowserRouter } from 'react-router-dom';

import { AppLayout } from '../../components/layout/AppLayout';
import { HomePage } from '../../pages/HomePage';
import { NotFoundPage } from '../../pages/NotFoundPage';

// A página de detalhe carrega o Recharts. Mantê-la em chunk separado evita que
// a home baixe a biblioteca de gráficos antes de o usuário abrir um dashboard.
const DatasetDetailPage = lazy(() =>
  import('../../pages/DatasetDetailPage').then((module) => ({ default: module.DatasetDetailPage })),
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
        path: '*',
        element: <NotFoundPage />,
      },
    ],
  },
]);
