<template>
    <nav
        class="navbar"
        role="navigation"
        aria-label="main navigation"
    >
        <div class="navbar-brand">
            <RouterLink
                :to="{ name: 'home' }"
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
                    :to="{ name: 'status' }"
                    class="navbar-item"
                    active-class="is-active"
                >
                    Status
                </RouterLink>
                <RouterLink
                    :to="{ name: 'configure' }"
                    class="navbar-item"
                    active-class="is-active"
                >
                    Configure
                </RouterLink>
                <RouterLink
                    :to="{ name: 'backup' }"
                    class="navbar-item"
                    active-class="is-active"
                >
                    Backup
                </RouterLink>
                <RouterLink
                    :to="{ name: 'restore' }"
                    class="navbar-item"
                    active-class="is-active"
                >
                    Restore
                </RouterLink>
            </div>
        </div>
    </nav>
    <div
        id="main-router-view"
        class="section status-open"
    >
        <RouterView />
    </div>
    <div
        id="status-footer"
        class="status-open"
    >
        Status Bar
    </div>
    <ModalsContainer />
</template>

<style>
#main-router-view.status-open {
    margin-bottom: 6em;
}
#status-footer {
    overflow: hidden;
    position: fixed;
    bottom: 0;
    width: 100%;
    height: 0;
    background-color: var(--body-background-color);
    transition: height 0.5s linear;

    display: flex;
    justify-content: center;
    align-items: center;
}
#status-footer.status-open {
    height: 6em;
    border-top: 2px solid var(--border);
}
</style>

<script setup lang="ts">
import { ref } from "vue";
import { RouterView } from "vue-router";
import { ModalsContainer } from "vue-final-modal";

const navbarActive = ref(false);
</script>
