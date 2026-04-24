import { createApp } from 'vue'
import { createPinia } from 'pinia'
import { createRouter, createWebHistory } from 'vue-router'
import App from './App.vue'

// 页面组件（后续填充）
import Dashboard from './pages/Dashboard.vue'
import TaskPage from './pages/TaskPage.vue'
import DataPage from './pages/DataPage.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', component: Dashboard },
    { path: '/tasks', component: TaskPage },
    { path: '/data', component: DataPage },
  ],
})

const pinia = createPinia()
const app = createApp(App)

app.use(pinia)
app.use(router)
app.mount('#app')