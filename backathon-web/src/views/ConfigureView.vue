<template>
    <h1 class="title is-2">Configure</h1>
    <div class="tabs">
        <ul>
            <RouterLinkListItem :to="{ name: 'configure-backup' }">
                Backup Files
            </RouterLinkListItem>
            <RouterLinkListItem :to="{ name: 'configure-destination' }">
                Destination
            </RouterLinkListItem>
            <RouterLinkListItem :to="{ name: 'configure-schedule' }">
                Schedule
            </RouterLinkListItem>
        </ul>
    </div>
    <div class="block">
        <router-view />
    </div>
</template>

<style scoped></style>

<script setup lang="ts">
import RouterLinkListItem from "@/utils/RouterLinkListItem.vue";
</script>
