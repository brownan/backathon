<template>
    <nav
        class="navbar"
        role="navigation"
        aria-label="main navigation"
    >
        <div class="navbar-brand">
            <RouterLink
                to="home"
                class="navbar-item"
            >
                <h1 class="title">Backathon</h1>
            </RouterLink>
            <a
                role="button"
                class="navbar-burger"
                :class="navbarActive ? 'is-active' : ''"
                aria-label="menu"
                aria-expanded="false"
                data-target="navbar-content"
                v-on:click="navbarActive = !navbarActive"
            >
                <span aria-hidden="true"></span>
                <span aria-hidden="true"></span>
                <span aria-hidden="true"></span>
                <span aria-hidden="true"></span>
            </a>
        </div>

        <div
            id="navbar-content"
            class="navbar-menu"
            :class="navbarActive ? 'is-active' : ''"
        >
            <div class="navbar-start">
                <RouterLink
                    to="status"
                    class="navbar-item"
                    active-class="is-active"
                >
                    Status
                </RouterLink>
                <RouterLink
                    to="configure"
                    class="navbar-item"
                    active-class="is-active"
                >
                    Configure
                </RouterLink>
                <RouterLink
                    to="backup"
                    class="navbar-item"
                    active-class="is-active"
                >
                    Backup
                </RouterLink>
                <RouterLink
                    to="restore"
                    class="navbar-item"
                    active-class="is-active"
                >
                    Restore
                </RouterLink>
            </div>
        </div>
    </nav>
    <div class="section">
        <RouterView />
    </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import { RouterView } from "vue-router";

const navbarActive = ref(false);
</script>
