import { createBrowserRouter } from 'react-router-dom';

import { AppLayout } from '../../components/layout/AppLayout';
import { DatasetDetailPage } from '../../pages/DatasetDetailPage';
import { HomePage } from '../../pages/HomePage';
import { NotFoundPage } from '../../pages/NotFoundPage';

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
        element: <DatasetDetailPage />,
      },
      {
        path: '*',
        element: <NotFoundPage />,
      },
    ],
  },
]);
