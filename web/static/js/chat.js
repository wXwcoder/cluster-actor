/**
 * 聊天室前端JavaScript逻辑
 * 管理WebSocket连接、消息收发、房间管理等功能
 */

class ChatApp {
    constructor() {
        // 从sessionStorage获取用户信息
        this.userId = sessionStorage.getItem('userId');
        this.username = sessionStorage.getItem('username');
        this.sessionId = sessionStorage.getItem('sessionId');
        
        // 如果没有用户信息，重定向到登录页
        if (!this.userId || !this.username) {
            window.location.href = '/';
            return;
        }
        
        // WebSocket连接
        this.ws = null;
        this.reconnectTimer = null;
        this.heartbeatTimer = null;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 5;
        
        // 状态管理
        this.currentRoom = null;
        this.rooms = [];
        this.roomUsers = [];
        this.messages = [];
        
        // DOM元素
        this.elements = {
            connectionStatus: document.getElementById('connectionStatus'),
            userInfo: document.getElementById('userInfo'),
            logoutBtn: document.getElementById('logoutBtn'),
            roomList: document.getElementById('roomList'),
            createRoomBtn: document.getElementById('createRoomBtn'),
            createRoomModal: document.getElementById('createRoomModal'),
            createRoomForm: document.getElementById('createRoomForm'),
            chatHeader: document.getElementById('chatHeader'),
            currentRoomName: document.getElementById('currentRoomName'),
            roomUserCount: document.getElementById('roomUserCount'),
            messageList: document.getElementById('messageList'),
            messageText: document.getElementById('messageText'),
            sendBtn: document.getElementById('sendBtn'),
            userList: document.getElementById('userList')
        };
        
        this.init();
    }
    
    /**
     * 初始化应用
     */
    init() {
        this.bindEvents();
        this.connectWebSocket();
        this.updateUserInfo();
    }
    
    /**
     * 绑定事件监听器
     */
    bindEvents() {
        // 退出登录
        this.elements.logoutBtn.addEventListener('click', () => this.logout());
        
        // 创建房间按钮
        this.elements.createRoomBtn.addEventListener('click', () => this.showCreateRoomModal());
        
        // 创建房间表单提交
        this.elements.createRoomForm.addEventListener('submit', (e) => {
            e.preventDefault();
            this.createRoom();
        });
        
        // 发送消息按钮
        this.elements.sendBtn.addEventListener('click', () => this.sendMessage());
        
        // 回车发送消息
        this.elements.messageText.addEventListener('keypress', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                this.sendMessage();
            }
        });
    }
    
    /**
     * 更新用户信息显示
     */
    updateUserInfo() {
        this.elements.userInfo.textContent = this.username;
    }
    
    /**
     * 连接WebSocket
     */
    connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws`;
        
        try {
            this.ws = new WebSocket(wsUrl);
            
            this.ws.onopen = () => this.onWebSocketOpen();
            this.ws.onmessage = (event) => this.onWebSocketMessage(event);
            this.ws.onclose = () => this.onWebSocketClose();
            this.ws.onerror = (error) => this.onWebSocketError(error);
        } catch (error) {
            console.error('WebSocket连接失败:', error);
            this.scheduleReconnect();
        }
    }
    
    /**
     * WebSocket连接成功
     */
    onWebSocketOpen() {
        console.log('WebSocket连接成功');
        this.updateConnectionStatus(true);
        this.reconnectAttempts = 0;
        this.startHeartbeat();
        
        // 发送登录消息
        this.send({
            type: 'login',
            payload: {
                user_id: parseInt(this.userId),
                username: this.username,
                session_id: this.sessionId
            }
        });
    }
    
    /**
     * 处理WebSocket消息
     */
    onWebSocketMessage(event) {
        try {
            const message = JSON.parse(event.data);
            this.handleMessage(message);
        } catch (error) {
            console.error('消息解析失败:', error);
        }
    }
    
    /**
     * 处理收到的消息
     */
    handleMessage(message) {
        switch (message.type) {
            case 'login_success':
                this.onLoginSuccess(message.payload);
                break;
            case 'login_error':
                this.onLoginError(message.payload);
                break;
            case 'logout_success':
                this.onLogoutSuccess();
                break;
            case 'room_created':
                this.onRoomCreated(message.payload);
                break;
            case 'room_joined':
                this.onRoomJoined(message.payload);
                break;
            case 'room_left':
                this.onRoomLeft();
                break;
            case 'room_list':
                this.onRoomList(message.payload);
                break;
            case 'room_users':
                this.onRoomUsers(message.payload);
                break;
            case 'new_message':
                this.onNewMessage(message.payload);
                break;
            case 'error':
                this.onError(message.payload);
                break;
            default:
                console.log('未知消息类型:', message.type);
        }
    }
    
    /**
     * WebSocket连接关闭
     */
    onWebSocketClose() {
        console.log('WebSocket连接关闭');
        this.updateConnectionStatus(false);
        this.stopHeartbeat();
        this.scheduleReconnect();
    }
    
    /**
     * WebSocket错误
     */
    onWebSocketError(error) {
        console.error('WebSocket错误:', error);
    }
    
    /**
     * 登录成功
     */
    onLoginSuccess(user) {
        console.log('登录成功:', user);
        // 获取房间列表
        this.getRoomList();
    }
    
    /**
     * 登录失败
     */
    onLoginError(payload) {
        console.error('登录失败:', payload.message);
        alert('登录失败: ' + payload.message);
    }
    
    /**
     * 登出成功
     */
    onLogoutSuccess() {
        sessionStorage.clear();
        window.location.href = '/';
    }
    
    /**
     * 房间创建成功
     */
    onRoomCreated(room) {
        console.log('房间创建成功:', room);
        this.closeCreateRoomModal();
        this.getRoomList();
    }
    
    /**
     * 加入房间成功
     */
    onRoomJoined(data) {
        console.log('加入房间成功:', data);
        this.currentRoom = data.room;
        this.updateChatHeader();
        this.loadMessages(data.recent_messages || []);
        this.getRoomUsers();
    }
    
    /**
     * 离开房间成功
     */
    onRoomLeft() {
        console.log('离开房间成功');
        this.currentRoom = null;
        this.messages = [];
        this.roomUsers = [];
        this.updateChatHeader();
        this.clearMessages();
        this.clearUserList();
    }
    
    /**
     * 收到房间列表
     */
    onRoomList(data) {
        console.log('收到房间列表:', data);
        this.rooms = data.rooms || [];
        this.renderRoomList();
    }
    
    /**
     * 收到房间用户列表
     */
    onRoomUsers(users) {
        console.log('收到房间用户列表:', users);
        this.roomUsers = users || [];
        this.renderUserList();
    }
    
    /**
     * 收到新消息
     */
    onNewMessage(message) {
        console.log('收到新消息:', message);
        this.addMessage(message);
    }
    
    /**
     * 错误处理
     */
    onError(payload) {
        console.error('错误:', payload.message);
        alert('错误: ' + payload.message);
    }
    
    /**
     * 发送WebSocket消息
     */
    send(message) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify(message));
        } else {
            console.error('WebSocket未连接');
        }
    }
    
    /**
     * 开始心跳
     */
    startHeartbeat() {
        this.heartbeatTimer = setInterval(() => {
            if (this.ws && this.ws.readyState === WebSocket.OPEN) {
                this.send({ type: 'ping' });
            }
        }, 30000); // 每30秒发送一次心跳
    }
    
    /**
     * 停止心跳
     */
    stopHeartbeat() {
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }
    }
    
    /**
     * 计划重连
     */
    scheduleReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.log('达到最大重连次数');
            return;
        }
        
        this.reconnectAttempts++;
        const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
        
        console.log(`将在 ${delay}ms 后尝试重连 (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
        
        this.reconnectTimer = setTimeout(() => {
            this.connectWebSocket();
        }, delay);
    }
    
    /**
     * 更新连接状态显示
     */
    updateConnectionStatus(connected) {
        if (connected) {
            this.elements.connectionStatus.textContent = '已连接';
            this.elements.connectionStatus.className = 'connection-status connected';
        } else {
            this.elements.connectionStatus.textContent = '未连接';
            this.elements.connectionStatus.className = 'connection-status disconnected';
        }
    }
    
    /**
     * 退出登录
     */
    logout() {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.send({ type: 'logout' });
        }
        sessionStorage.clear();
        window.location.href = '/';
    }
    
    /**
     * 获取房间列表
     */
    getRoomList() {
        this.send({
            type: 'get_room_list',
            payload: {
                page: 1,
                page_size: 50
            }
        });
    }
    
    /**
     * 渲染房间列表
     */
    renderRoomList() {
        this.elements.roomList.innerHTML = '';
        
        if (this.rooms.length === 0) {
            this.elements.roomList.innerHTML = '<p class="text-muted">暂无房间</p>';
            return;
        }
        
        this.rooms.forEach(room => {
            const roomEl = document.createElement('div');
            roomEl.className = 'room-item';
            if (this.currentRoom && this.currentRoom.room_id === room.room_id) {
                roomEl.classList.add('active');
            }
            
            roomEl.innerHTML = `
                <div class="room-name">${this.escapeHtml(room.room_name)}</div>
                <div class="room-info">
                    ${room.current_members}/${room.max_members} 人 | 
                    创建者: ${this.escapeHtml(room.creator_name)}
                </div>
            `;
            
            roomEl.addEventListener('click', () => this.joinRoom(room.room_id));
            this.elements.roomList.appendChild(roomEl);
        });
    }
    
    /**
     * 加入房间
     */
    joinRoom(roomId) {
        // 如果已经在房间中，先离开
        if (this.currentRoom) {
            this.send({ type: 'leave_room' });
        }
        
        this.send({
            type: 'join_room',
            payload: {
                room_id: roomId
            }
        });
    }
    
    /**
     * 创建房间
     */
    createRoom() {
        const roomName = document.getElementById('roomName').value;
        const maxMembers = parseInt(document.getElementById('maxMembers').value);
        
        if (!roomName) {
            alert('请输入房间名称');
            return;
        }
        
        this.send({
            type: 'create_room',
            payload: {
                room_name: roomName,
                max_members: maxMembers
            }
        });
    }
    
    /**
     * 显示创建房间模态框
     */
    showCreateRoomModal() {
        this.elements.createRoomModal.classList.add('show');
    }
    
    /**
     * 关闭创建房间模态框
     */
    closeCreateRoomModal() {
        this.elements.createRoomModal.classList.remove('show');
        this.elements.createRoomForm.reset();
    }
    
    /**
     * 获取房间用户列表
     */
    getRoomUsers() {
        this.send({
            type: 'get_room_users'
        });
    }
    
    /**
     * 发送消息
     */
    sendMessage() {
        const content = this.elements.messageText.value.trim();
        
        if (!content) {
            return;
        }
        
        if (!this.currentRoom) {
            alert('请先加入房间');
            return;
        }
        
        this.send({
            type: 'send_message',
            payload: {
                content: content
            }
        });
        
        this.elements.messageText.value = '';
    }
    
    /**
     * 更新聊天头部
     */
    updateChatHeader() {
        if (this.currentRoom) {
            this.elements.currentRoomName.textContent = this.currentRoom.room_name;
            this.elements.roomUserCount.textContent = 
                `${this.currentRoom.current_members}/${this.currentRoom.max_members} 人`;
        } else {
            this.elements.currentRoomName.textContent = '选择一个房间';
            this.elements.roomUserCount.textContent = '';
        }
    }
    
    /**
     * 加载消息
     */
    loadMessages(messages) {
        this.messages = messages || [];
        this.clearMessages();
        this.messages.forEach(msg => this.addMessage(msg, false));
    }
    
    /**
     * 添加消息
     */
    addMessage(message, scroll = true) {
        this.messages.push(message);
        this.renderMessage(message);
        
        if (scroll) {
            this.scrollToBottom();
        }
    }
    
    /**
     * 渲染消息
     */
    renderMessage(message) {
        const messageEl = document.createElement('div');
        
        // 判断消息类型
        if (message.type === 3 || message.type === 4) {
            // 系统消息（加入/离开）
            messageEl.className = 'message system';
            messageEl.textContent = message.content;
        } else if (message.sender_id == this.userId) {
            // 自己发送的消息
            messageEl.className = 'message sent';
            messageEl.innerHTML = `
                <div class="message-content">${this.escapeHtml(message.content)}</div>
                <div class="message-header">
                    <span class="message-time">${this.formatTime(message.timestamp)}</span>
                </div>
            `;
        } else {
            // 他人发送的消息
            messageEl.className = 'message received';
            messageEl.innerHTML = `
                <div class="message-header">
                    <span class="message-sender">${this.escapeHtml(message.sender_name)}</span>
                    <span class="message-time">${this.formatTime(message.timestamp)}</span>
                </div>
                <div class="message-content">${this.escapeHtml(message.content)}</div>
            `;
        }
        
        this.elements.messageList.appendChild(messageEl);
    }
    
    /**
     * 清空消息
     */
    clearMessages() {
        this.elements.messageList.innerHTML = '';
    }
    
    /**
     * 渲染用户列表
     */
    renderUserList() {
        this.elements.userList.innerHTML = '';
        
        if (this.roomUsers.length === 0) {
            this.elements.userList.innerHTML = '<p class="text-muted">暂无用户</p>';
            return;
        }
        
        this.roomUsers.forEach(user => {
            const userEl = document.createElement('div');
            userEl.className = 'user-item';
            
            const avatar = document.createElement('div');
            avatar.className = 'user-avatar';
            avatar.textContent = user.username.charAt(0).toUpperCase();
            
            const name = document.createElement('div');
            name.className = 'user-name';
            name.textContent = user.username;
            
            userEl.appendChild(avatar);
            userEl.appendChild(name);
            this.elements.userList.appendChild(userEl);
        });
    }
    
    /**
     * 清空用户列表
     */
    clearUserList() {
        this.elements.userList.innerHTML = '';
    }
    
    /**
     * 滚动到底部
     */
    scrollToBottom() {
        this.elements.messageList.scrollTop = this.elements.messageList.scrollHeight;
    }
    
    /**
     * 格式化时间
     */
    formatTime(timestamp) {
        const date = new Date(timestamp);
        const hours = date.getHours().toString().padStart(2, '0');
        const minutes = date.getMinutes().toString().padStart(2, '0');
        return `${hours}:${minutes}`;
    }
    
    /**
     * HTML转义
     */
    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

/**
 * 关闭创建房间模态框（全局函数）
 */
function closeCreateRoomModal() {
    const modal = document.getElementById('createRoomModal');
    if (modal) {
        modal.classList.remove('show');
    }
}

// 页面加载完成后初始化应用
document.addEventListener('DOMContentLoaded', () => {
    new ChatApp();
});
