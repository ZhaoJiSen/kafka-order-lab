import React from 'react';
import ReactDOM from 'react-dom/client';
import { RouterProvider, createRouter, createRootRoute, createRoute } from '@tanstack/react-router';
import { Home, Layout, LogsPage, PartitionPage } from './App';
import '@xyflow/react/dist/style.css';
import './styles/base.css';
import './styles/lab.scss';
const root = createRootRoute({ component: Layout, notFoundComponent: () => <main className="standalone-page"><h1>页面不存在</h1><a href="/">返回订单实验台</a></main> });
const index = createRoute({ getParentRoute: () => root, path: '/', component: Home });
const partitions = createRoute({ getParentRoute: () => root, path: '/partitions', component: PartitionPage });
const logs = createRoute({ getParentRoute: () => root, path: '/logs', component: LogsPage });
const router = createRouter({ routeTree: root.addChildren([index, partitions, logs]) });
declare module '@tanstack/react-router' { interface Register { router: typeof router } }
ReactDOM.createRoot(document.getElementById('root')!).render(<React.StrictMode><RouterProvider router={router}/></React.StrictMode>);
