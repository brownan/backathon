import pluginVue from './backathon-web/node_modules/eslint-plugin-vue'
import { defineConfigWithVueTs, vueTsConfigs } from './backathon-web/node_modules/@vue/eslint-config-typescript'
import skipFormatting from './backathon-web/node_modules/@vue/eslint-config-prettier/skip-formatting'

// To allow more languages other than `ts` in `.vue` files, uncomment the following lines:
// import { configureVueProject } from '@vue/eslint-config-typescript'
// configureVueProject({ scriptLangs: ['ts', 'tsx'] })
// More info at https://github.com/vuejs/eslint-config-typescript/#advanced-setup

export default defineConfigWithVueTs(
    {
        name: 'app/files-to-lint',
        files: ['**/*.{ts,mts,tsx,vue}'],
    },

    {
        name: 'app/files-to-ignore',
        ignores: ['**/dist/**', '**/dist-ssr/**', '**/coverage/**'],
    },

    pluginVue.configs['flat/essential'],
    vueTsConfigs.recommended,
    skipFormatting,
    {
        rules: {
            curly: ['warn', 'all'],
            'vue/component-tags-order': [
                'error',
                {
                    order: ['template', 'style', 'script'],
                },
            ],
            'vue/html-self-closing': [
                'warn',
                {
                    html: {
                        normal: 'never',
                        void: 'any',
                    },
                },
            ],

            'vue/v-bind-style': 'off',
            'vue/v-on-style': 'off',
            'no-console': 'off',
            'no-unused-vars': [
                'error',
                {
                    args: 'none',
                },
            ],
            eqeqeq: ['warn'],
            'no-var': ['warn'],
        },
    },
)
