import { createRouter, createWebHistory } from "vue-router";
import HomeView from "@/views/HomeView.vue";
import StatusView from "@/views/StatusView.vue";
import ConfigureView from "@/views/ConfigureView.vue";
import BackupView from "@/views/BackupView.vue";
import RestoreView from "@/views/RestoreView.vue";
import ConfigureBackup from "@/components/ConfigureBackup.vue";
import ConfigureDestination from "@/components/ConfigureDestination.vue";
import ConfigureSchedule from "@/components/ConfigureSchedule.vue";

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
    ],
});

export default router;
