import { createRouter, createWebHistory } from 'vue-router'
import HomeView from '../views/HomeView.vue'
import StatusView from '@/views/StatusView.vue'
import ConfigureView from '@/views/ConfigureView.vue'
import BackupView from '@/views/BackupView.vue'
import RestoreView from '@/views/RestoreView.vue'

const router = createRouter({
    history: createWebHistory(import.meta.env.BASE_URL),
    routes: [
        {
            path: '/',
            name: 'home',
            component: HomeView,
        },
        {
            path: '/status',
            name: 'status',
            component: StatusView,
        },
        {
            path: '/configure',
            name: 'configure',
            component: ConfigureView,
        },
        {
            path: '/backup',
            name: 'backup',
            component: BackupView,
        },
        {
            path: '/restore',
            name: 'restore',
            component: RestoreView,
        },
    ],
})

export default router
