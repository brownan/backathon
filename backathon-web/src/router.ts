import { createRouter, createWebHistory } from "vue-router";
import HomeView from "@/views/HomeView.vue";
import StatusView from "@/views/StatusView.vue";
import ConfigureView from "@/views/ConfigureView.vue";
import BackupView from "@/views/BackupView.vue";
import RestoreView from "@/views/RestoreView.vue";
import MaintenanceView from "@/views/MaintenanceView.vue";
import ConfigureBackup from "@/views/ConfigureBackup.vue";
import ConfigureDestination from "@/views/ConfigureDestination.vue";
import ConfigureSchedule from "@/views/ConfigureSchedule.vue";
import RetentionView from "@/views/RetentionView.vue";

const router = createRouter({
    history: createWebHistory(import.meta.env.BASE_URL),
    routes: [
        {
            path: "/",
            name: "home",
            component: HomeView,
        },
        {
            path: "/status",
            name: "status",
            component: StatusView,
        },
        {
            path: "/configure",
            name: "configure",
            component: ConfigureView,
            redirect: { name: "configure-backup" },
            children: [
                {
                    path: "backup",
                    component: ConfigureBackup,
                    name: "configure-backup",
                },
                {
                    path: "destination",
                    component: ConfigureDestination,
                    name: "configure-destination",
                },
                {
                    path: "schedule",
                    component: ConfigureSchedule,
                    name: "configure-schedule",
                },
                {
                    path: "retention",
                    component: RetentionView,
                    name: "configure-retention",
                },
            ],
        },
        {
            path: "/backup",
            name: "backup",
            component: BackupView,
        },
        {
            path: "/restore/:id(\\d+)?",
            name: "restore",
            component: RestoreView,
        },
        {
            path: "/maintenance",
            name: "maintenance",
            component: MaintenanceView,
        },
    ],
});

export default router;
