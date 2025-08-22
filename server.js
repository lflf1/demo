const express = require('express');
const axios = require('axios');
const axiosRetry = require('axios-retry');
const cors = require('cors');
const mysql = require('mysql2/promise');
const config = require('./config');
const logger = require('./logger');
const cron = require('node-cron');
const NodeCache = require('node-cache');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const XLSX = require('xlsx'); 
const crypto = require('crypto'); 
const https = require('https');
const archiver = require('archiver');

// 验证配置文件和日志模块
if (!config || !config.server || !config.database || !config.externalApi) {
    throw new Error('配置文件缺失或配置不完整');
}
if (!logger) {
    throw new Error('日志模块未正确加载');
}

// 创建 axios 实例并配置重试机制
const instance = axios.create();
axiosRetry(instance, { retries: 3 });

// 创建 data 文件夹（如果不存在）
const dataDir = path.join(__dirname, 'data');
if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir);
}

// 配置 multer 以将文件保存到 data 文件夹
const storage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, dataDir);
    },
    filename: function (req, file, cb) {
        cb(null, file.originalname);
    }
});

const upload = multer({ storage: storage });

const app = express();
app.use(express.json());
// 确保CORS配置包含token头
app.use(cors({
    exposedHeaders: ['token']
}));
const port = config.server.port;
const myCache = new NodeCache({ stdTTL: 86400 }); // 缓存有效期为一天

// 创建MySQL连接池（带SSL配置）
const pool = mysql.createPool({
    host: config.database.host,
    user: config.database.user,
    password: config.database.password,
    database: config.database.database,
    waitForConnections: true,
    connectionLimit: 20,
    queueLimit: 1000,
    timezone: '+08:00',
    charset: 'utf8mb4',
    ssl: config.database.ssl ? { rejectUnauthorized: false } : null
});

// 添加哈希验证中间件
app.use(async (req, res, next) => {
    // 定义无需验证的公共路径列表，包含静态资源路径
    const publicPaths = [
        '/login', 
        '/getConfigHash',
        '/a',
        '/b',
        '/a-progress',
        '/getcustomer_balance',
        '/getweek_dingdan',
        '/get_meixi',
        '/ruku',
        '/sku_kc',
        '/get_fuhai',
        '/post_fuhai',
        '/get-fedex-cookies',
        '/post_cookie',
        '/track-amazon',
        '/css/',   // 允许所有CSS文件
        '/js/',    // 允许所有JS文件
        '/images/', // 允许所有图片文件
        '/fonts/',  // 允许所有字体文件
        '/customer_info',
        '/jianhuo_dataa',
        '/jianhuo_data',
        '/jianhuo_datab',  
        '/customer_info1',
        '/pj_submit',
        '/feedback_check',
        '/server_time'
    ];
    
    // 检查请求路径是否属于无需验证的公共路径
    const isPublic = publicPaths.some(path => 
        req.path === path || 
        (path.endsWith('/') && req.path.startsWith(path))
    );
    
    // 新增：检查是否为HTML文件
    const isHtml = req.path.endsWith('.html');
    
    // 如果是公共路径或HTML文件，直接放行
    if (isPublic || isHtml) return next();

    // 非公共路径需要验证token
    const clientHash = req.headers['token'];
    
    // 检查请求头中是否包含token
    if (!clientHash) {
        return res.status(401).json({
            error: '请提供有效的密钥'
        });
    }

    // 从连接池获取数据库连接
    const conn = await pool.getConnection();
    try {
        // 查询数据库中是否存在该哈希值
        const [rows] = await conn.query('SELECT id FROM users WHERE password_hash = ?', [clientHash]);

        // 如果数据库中不存在该哈希值，返回验证失败
        if (rows.length === 0) {
            return res.status(401).json({
                error: '密钥不匹配，请重新登录获取最新密钥'
            });
        }

        // 哈希值验证通过，继续处理请求
        next();
    } catch (error) {
        // 记录错误日志并返回服务器内部错误
        logger.error(`密钥验证出错: ${error.stack}`);
        res.status(500).json({
            success: false,
            error: '服务器内部错误'
        });
    } finally {
        // 释放数据库连接
        conn.release();
    }
});


// 添加哈希验证接口
app.get('/verifyHash', async (req, res) => {
    try {
        const clientHash = req.query.hash;
        if (!clientHash) {
            return res.status(401).json({ 
                success: false,
                error: '缺少密钥' 
            });
        }

        const conn = await pool.getConnection(); // 添加连接获取
        try {
            const [rows] = await conn.query(
                'SELECT id FROM users WHERE password_hash = ?', 
                [clientHash]
            );
            
            if (rows.length === 0) {
                return res.json({ 
                    success: false,
                    error: 'ERROR' 
                });
            }

            res.json({ 
                success: true,
                message: 'TRUE' 
            });
        } finally {
            conn.release(); // 确保连接释放
        }
    } catch (error) {
        console.error(error); // 添加详细错误日志
        res.status(500).json({ 
            success: false,
            error: '服务器错误',
            details: error.message // 返回具体错误信息
        });
    }
});


// 新增：从数据库查询符合条件的customer_code
async function fetchDynamicCustomerCodes() {
    const conn = await pool.getConnection();
    try {
        // 执行目标SQL：排除以下客户
        // 1. customer_name以DK开头
        // 2. customer_name以XXS开头
        // 3. customer_name包含test（不区分大小写）
        const [rows] = await conn.query(
            `SELECT customer_code 
             FROM customer_info 
             WHERE customer_name NOT LIKE ? 
               AND customer_name NOT LIKE ?
               AND LOWER(customer_name) NOT LIKE ?
             ORDER BY customer_code`,
            ['DK%', 'XXS%', '%test%'] // 参数化查询，避免SQL注入
        );

        // 提取customer_code字段，组成数组并拼接为逗号分隔的字符串
        const customerCodes = rows.map(row => row.customer_code).join(',');
        logger.info(`动态获取到${rows.length}个customer_code`);

        // 更新配置中的customerCodes
        config.externalApi.requestTemplate.customerCodes = customerCodes;
        return customerCodes;
    } catch (error) {
        logger.error(`动态获取customerCodes失败: ${error.stack}`);
        // 设置默认值，避免应用启动失败
        config.externalApi.requestTemplate.customerCodes = "1440001,1440002,1440003";
    } finally {
        conn.release(); // 释放数据库连接
    }
}
    

// 封装日期格式化函数
function formatDate(date) {
    return [
        date.getFullYear(),
        (date.getMonth() + 1).toString().padStart(2, '0'),
        date.getDate().toString().padStart(2, '0')
    ].join('-');
}

// 初始化数据库表
async function initializeDatabase() {
    const conn = await pool.getConnection();
    try {
        logger.info('开始初始化数据库表');

        // 创建 hourly_orders 表（带注释和索引优化）
        await conn.query(`
            CREATE TABLE IF NOT EXISTS hourly_orders (
                id INT(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
                hour VARCHAR(5) NOT NULL COMMENT '小时(HH:MM)',
                total_orders INT(11) NOT NULL COMMENT '订单总量',
                date DATE NOT NULL COMMENT '日期(YYYY-MM-DD)',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                PRIMARY KEY (id),
                UNIQUE KEY idx_date_hour (date, hour),
                INDEX idx_hour (hour)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        `);
        logger.info('数据库表 hourly_orders 创建成功');

        // 创建 data 表
        await conn.query(`
            CREATE TABLE IF NOT EXISTS data (
                id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
                Warehouse VARCHAR(255),
                Client VARCHAR(255),
                Outbound_Order_No VARCHAR(255),
                Delivery_Order_No VARCHAR(255),
                Sale_Platform VARCHAR(255),
                Platform_Number VARCHAR(255),
                Outbound_discrepancy_adjustment_No VARCHAR(255),
                Reference_order_No VARCHAR(255),
                Signature VARCHAR(255),
                Insurance VARCHAR(255),
                Type_of_order_variety VARCHAR(255),
                Shipping_service VARCHAR(255),
                Shipping_Group_service VARCHAR(255),
                Label_Print_Status VARCHAR(255),
                Status VARCHAR(255),
                Order_source VARCHAR(255),
                Creation_time DATETIME,
                Create_wave_time DATETIME,
                Pick_time DATETIME,
                Review_time DATETIME,
                Weighing_time DATETIME,
                OutboundTime DATETIME,
                SKU_varieties INT,
                Total_Qty_of_SKU INT,
                Package_Qty INT,
                Remark TEXT,
                Recipient VARCHAR(255),
                Telephone VARCHAR(255),
                Email VARCHAR(255),
                Recipient_tax_ID VARCHAR(255),
                Company VARCHAR(255),
                Country_Region VARCHAR(255),
                Province_State VARCHAR(255),
                City VARCHAR(255),
                District VARCHAR(255),
                Post_code VARCHAR(255),
                House_No VARCHAR(255),
                Address1 VARCHAR(255),
                Address2 VARCHAR(255),
                Package_Material_Code_Quantity_WMS VARCHAR(255),
                Value_added_Service_Quantity VARCHAR(255),
                Tracking_Status VARCHAR(255),
                Transit_Day INT,
                Latest_Info TEXT,
                Update_Time DATETIME,
                Receipt_Time DATETIME,
                Delivered_Time DATETIME,
                Current_Sales_representative VARCHAR(255),
                Current_Customer_service_representative VARCHAR(255),
                Package_1_Tracking_No VARCHAR(255),
                Package_1_Package_Size VARCHAR(255),
                Package_1_Package_Weight VARCHAR(255),
                SKU_1_SKU VARCHAR(255),
                SKU_1_Product_Name VARCHAR(255),
                SKU_1_Product_Type VARCHAR(255),
                SKU_1_Length VARCHAR(255),
                SKU_1_Width VARCHAR(255),
                SKU_1_Height VARCHAR(255),
                SKU_1_Unit VARCHAR(255),
                SKU_1_Weight VARCHAR(255),
                SKU_1_Outbound_Qty INT,
                SKU_2_SKU VARCHAR(255),
                SKU_2_Product_Name VARCHAR(255),
                SKU_2_Product_Type VARCHAR(255),
                SKU_2_Length VARCHAR(255),
                SKU_2_Width VARCHAR(255),
                SKU_2_Height VARCHAR(255),
                SKU_2_Unit VARCHAR(255),
                SKU_2_Weight VARCHAR(255),
                SKU_2_Outbound_Qty INT,
                SKU_3_SKU VARCHAR(255),
                SKU_3_Product_Name VARCHAR(255),
                SKU_3_Product_Type VARCHAR(255),
                SKU_3_Length VARCHAR(255),
                SKU_3_Width VARCHAR(255),
                SKU_3_Height VARCHAR(255),
                SKU_3_Unit VARCHAR(255),
                SKU_3_Weight VARCHAR(255),
                SKU_3_Outbound_Qty INT,
                SKU_4_SKU VARCHAR(255),
                SKU_4_Product_Name VARCHAR(255),
                SKU_4_Product_Type VARCHAR(255),
                SKU_4_Length VARCHAR(255),
                SKU_4_Width VARCHAR(255),
                SKU_4_Height VARCHAR(255),
                SKU_4_Unit VARCHAR(255),
                SKU_4_Weight VARCHAR(255),
                SKU_4_Outbound_Qty INT,
                SKU_5_SKU VARCHAR(255),
                SKU_5_Product_Name VARCHAR(255),
                SKU_5_Product_Type VARCHAR(255),
                SKU_5_Length VARCHAR(255),
                SKU_5_Width VARCHAR(255),
                SKU_5_Height VARCHAR(255),
                SKU_5_Unit VARCHAR(255),
                SKU_5_Weight VARCHAR(255),
                SKU_5_Outbound_Qty INT,
                SKU_6_SKU VARCHAR(255),
                SKU_6_Product_Name VARCHAR(255),
                SKU_6_Product_Type VARCHAR(255),
                SKU_6_Length VARCHAR(255),
                SKU_6_Width VARCHAR(255),
                SKU_6_Height VARCHAR(255),
                SKU_6_Unit VARCHAR(255),
                SKU_6_Weight VARCHAR(255),
                SKU_6_Outbound_Qty INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        `);
        logger.info('数据库表 data 创建成功');

        // 清理旧数据（保留3天）
        await conn.query(`
            DELETE FROM hourly_orders 
            WHERE date < DATE_SUB(CONVERT_TZ(NOW(), '+00:00', '+08:00'), INTERVAL 3 DAY)
              OR hour NOT REGEXP '^[0-9]{2}:[0-9]{2}$'
        `);
        logger.info('旧数据清理成功');

        logger.info('数据库初始化完成');
    } catch (error) {
        logger.error(`数据库初始化失败: ${error.stack}`);
    } finally {
        conn.release();
    }
}

// 获取精确北京时间（优化时区处理）
function getBeijingTime() {
    const now = new Date();
    return new Date(now.getTime() + (now.getTimezoneOffset() * 60000) + (8 * 3600000));
}

// 数据获取逻辑
async function fetchData() {
    try {
        const beijingTime = getBeijingTime();
        const currentDate = formatDate(beijingTime);
        const currentHour = `${beijingTime.getHours().toString().padStart(2, '0')}:00`;

        const response = await instance.post(config.externalApi.url, {
            ...config.externalApi.requestTemplate,
            startTime: `${currentDate} 00:00:00`,
            endTime: `${currentDate} 23:59:59`
        }, {
            headers: config.externalApi.headers,
            timeout: 15000
        });

        const totalOrders = Math.max(0, parseInt(response.data?.data?.total || 0));

        return {
            date: currentDate,
            hour: currentHour,
            orders: totalOrders
        };
    } catch (error) {
        if (error.response) {
            // 服务器响应状态码非 2xx
            logger.error(`请求失败，状态码: ${error.response.status}，响应数据: ${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            // 有请求但无响应
            logger.error('请求发送成功，但没有收到响应');
        } else {
            // 设置请求时出错
            logger.error(`请求设置出错: ${error.message}`);
        }
        throw error;
    }
}

// 新增的数据获取逻辑，用于 /getoutdata 接口
async function fetchOutData() {
    try {
        const beijingTime = getBeijingTime();
        const prevDate = new Date(beijingTime);
        prevDate.setDate(prevDate.getDate() - 1);
        const prevDateStr = formatDate(prevDate);

        // 检查缓存
        const cachedData = myCache.get(prevDateStr);
        if (cachedData) {
            logger.info(`从缓存中获取 /getoutdata 数据，日期: ${prevDateStr}`);
            return cachedData;
        }

        const currentHour = `${beijingTime.getHours().toString().padStart(2, '0')}:00`;

        const response = await instance.post(config.externalApi.url, {
            ...config.externalApi.requestTemplate,
            timeType: "outboundTime",
            startTime: `${prevDateStr} 00:00:00`,
            endTime: `${prevDateStr} 23:59:59`
        }, {
            headers: config.externalApi.headers,
            timeout: 15000
        });

        const totalOrders = Math.max(0, parseInt(response.data?.data?.total || 0));

        const data = {
            date: prevDateStr,
            hour: currentHour,
            orders: totalOrders
        };

        // 将数据存入缓存
        myCache.set(prevDateStr, data);
        logger.info(`将 /getoutdata 数据存入缓存，日期: ${prevDateStr}`);

        return data;
    } catch (error) {
        if (error.response) {
            // 服务器响应状态码非 2xx
            logger.error(`请求失败，状态码: ${error.response.status}，响应数据: ${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            // 有请求但无响应
            logger.error('请求发送成功，但没有收到响应');
        } else {
            // 设置请求时出错
            logger.error(`请求设置出错: ${error.message}`);
        }
        throw error;
    }
}

// 新增的数据获取逻辑，用于 /getoutdata 接口
async function fetchOutData() {
    try {
        const beijingTime = getBeijingTime();
        const prevDate = new Date(beijingTime);
        prevDate.setDate(prevDate.getDate() - 1);
        const prevDateStr = formatDate(prevDate);

        // 检查缓存
        const cachedData = myCache.get(prevDateStr);
        if (cachedData) {
            logger.info(`从缓存中获取 /getoutdata 数据，日期: ${prevDateStr}`);
            return cachedData;
        }

        const currentHour = `${beijingTime.getHours().toString().padStart(2, '0')}:00`;

        const response = await instance.post(config.externalApi.url, {
            ...config.externalApi.requestTemplate,
            timeType: "outboundTime",
            startTime: `${prevDateStr} 00:00:00`,
            endTime: `${prevDateStr} 23:59:59`
        }, {
            headers: config.externalApi.headers,
            timeout: 15000
        });

        const totalOrders = Math.max(0, parseInt(response.data?.data?.total || 0));

        const data = {
            date: prevDateStr,
            hour: currentHour,
            orders: totalOrders
        };

        // 将数据存入缓存
        myCache.set(prevDateStr, data);
        logger.info(`将 /getoutdata 数据存入缓存，日期: ${prevDateStr}`);

        return data;
    } catch (error) {
        if (error.response) {
            // 服务器响应状态码非 2xx
            logger.error(`请求失败，状态码: ${error.response.status}，响应数据: ${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            // 有请求但无响应
            logger.error('请求发送成功，但没有收到响应');
        } else {
            // 设置请求时出错
            logger.error(`请求设置出错: ${error.message}`);
        }
        throw error;
    }
}

// 数据写入逻辑
async function writeData(data) {
    const conn = await pool.getConnection();
    try {
        // 使用高效插入/更新语句
        const [result] = await conn.query(`
            INSERT INTO hourly_orders (date, hour, total_orders)
            VALUES (?, ?, ?)
            ON DUPLICATE KEY UPDATE
                total_orders = VALUES(total_orders),
                updated_at = NOW()
        `, [data.date, data.hour, data.orders]);

        logger.info(`数据已更新 - 日期: ${data.date}, 小时: ${data.hour}, 订单数: ${data.orders}`);
    } catch (error) {
        logger.error(`数据写入失败: ${error.stack}`);
    } finally {
        conn.release();
    }
}

// 核心数据获取接口
app.get('/getData', async (req, res) => {
    try {
        const data = await fetchData();
        res.json({
            success: true,
            data
        });
    } catch (error) {
        logger.error(`数据获取失败: ${error.stack}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});

// 新增的 /getoutdata 接口
app.get('/getoutdata', async (req, res) => {
    try {
        const data = await fetchOutData();
        res.json({
            success: true,
            data
        });
    } catch (error) {
        logger.error(`/getoutdata 数据获取失败: ${error.stack}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});

// 当日数据查询接口（带缓存机制）
app.get('/getHourlyOrders', async (req, res) => {
    try {
        const beijingTime = getBeijingTime();
        const currentDate = formatDate(beijingTime);

        // 带索引的高效查询
        const [rows] = await pool.query(`
            SELECT 
                date, 
                hour, 
                total_orders AS orders 
            FROM hourly_orders
            WHERE date = ?
            ORDER BY 
                STR_TO_DATE(hour, '%H:%i') ASC
        `, [currentDate]);

        res.json({
            success: true,
            data: rows,
            meta: {
                generated_at: new Date().toISOString(),
                timezone: 'Asia/Shanghai'
            }
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: '数据查询失败'
        });
    }
});

// 新增的 geterror 接口
app.get('/geterror', async (req, res) => {
    try {
        const beijingTime = getBeijingTime();
        const currentDate = formatDate(beijingTime);

        const sevenDaysAgo = new Date(beijingTime);
        sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 6);
        const sevenDaysAgoStr = formatDate(sevenDaysAgo);

        // 首次请求获取总数据量
        const firstRequestData = {
            orderCreateTime: [`${sevenDaysAgoStr} 00:00:00`, `${currentDate} 23:59:59`],
            status: 5,
            current: 1,
            size: 200,
            total: 0,
            orderCreateStartTime: `${sevenDaysAgoStr} 00:00:00`,
            orderCreateEndTime: `${currentDate} 23:59:59`
        };
        const firstResponse = await instance.post('https://omp.xlwms.com/gateway/omp/order/oms/small/page', firstRequestData, {
            headers: config.externalApi.headers,
            timeout: 15000
        });

        const total = firstResponse.data.data.total;
        const pageCount = Math.ceil(total / 200);

        let allExtractedData = [];
        for (let currentPage = 1; currentPage <= pageCount; currentPage++) {
            const requestData = {
                orderCreateTime: [`${sevenDaysAgoStr} 00:00:00`, `${currentDate} 23:59:59`],
                status: 5,
                current: currentPage,
                size: 200,
                total: 0,
                orderCreateStartTime: `${sevenDaysAgoStr} 00:00:00`,
                orderCreateEndTime: `${currentDate} 23:59:59`
            };
            const response = await instance.post('https://omp.xlwms.com/gateway/omp/order/oms/small/page', requestData, {
                headers: config.externalApi.headers,
                timeout: 15000
            });

            // 检查 response.data.data.records 是否为数组
            let records = response.data.data.records;
            if (!Array.isArray(records)) {
                // 如果 records 不是数组，将其转换为数组或返回空数组
                if (records) {
                    records = [records];
                } else {
                    records = [];
                }
            }

            // 提取所需字段
            const extractedData = records.map(item => ({
                客户名称: item.customerName,
                客户代码: item.customerCode,
                仓库: item.whName,
                出库单号: item.outboundOrderNo,
                物流渠道组: item.logisticsChannelName,
                物流渠道: item.relLogisticsChannel,
                异常原因: item.exceptionDesc
            }));
            allExtractedData = allExtractedData.concat(extractedData);
        }

        res.json({
            success: true,
            data: allExtractedData
        });
    } catch (error) {
        if (error.response) {
            // 服务器响应状态码非 2xx
            logger.error(`请求失败，状态码: ${error.response.status}，响应数据: ${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            // 有请求但无响应
            logger.error('请求发送成功，但没有收到响应');
        } else {
            // 设置请求时出错
            logger.error(`请求设置出错: ${error.message}`);
        }
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});    

// 新增的 geterror1 接口
app.get('/geterror1', async (req, res) => {
    try {
        const beijingTime = getBeijingTime();
        const currentDate = formatDate(beijingTime);

        const sevenDaysAgo = new Date(beijingTime);
        sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 6);
        const sevenDaysAgoStr = formatDate(sevenDaysAgo);

        // 首次请求获取总数据量
        const firstRequestData = {
            orderCreateTime: [`${sevenDaysAgoStr} 00:00:00`, `${currentDate} 23:59:59`],
            status: 16,
            current: 1,
            size: 200,
            total: 0,
            orderCreateStartTime: `${sevenDaysAgoStr} 00:00:00`,
            orderCreateEndTime: `${currentDate} 23:59:59`
        };
        const firstResponse = await instance.post('https://omp.xlwms.com/gateway/omp/order/oms/small/page', firstRequestData, {
            headers: config.externalApi.headers,
            timeout: 15000
        });

        const total = firstResponse.data.data.total;
        const pageCount = Math.ceil(total / 200);

        let allExtractedData = [];
        for (let currentPage = 1; currentPage <= pageCount; currentPage++) {
            const requestData = {
                orderCreateTime: [`${sevenDaysAgoStr} 00:00:00`, `${currentDate} 23:59:59`],
                status: 16,
                current: currentPage,
                size: 200,
                total: 0,
                orderCreateStartTime: `${sevenDaysAgoStr} 00:00:00`,
                orderCreateEndTime: `${currentDate} 23:59:59`
            };
            const response = await instance.post('https://omp.xlwms.com/gateway/omp/order/oms/small/page', requestData, {
                headers: config.externalApi.headers,
                timeout: 15000
            });

            // 检查 response.data.data.records 是否为数组
            let records = response.data.data.records;
            if (!Array.isArray(records)) {
                // 如果 records 不是数组，将其转换为数组或返回空数组
                if (records) {
                    records = [records];
                } else {
                    records = [];
                }
            }

            // 提取所需字段
            const extractedData = records.map(item => ({
                客户名称: item.customerName,
                客户代码: item.customerCode,
                仓库: item.whName,
                出库单号: item.outboundOrderNo,
                物流渠道组: item.logisticsChannelName,
                物流渠道: item.relLogisticsChannel,
                异常原因: item.exceptionDesc
            }));
            allExtractedData = allExtractedData.concat(extractedData);
        }

        res.json({
            success: true,
            data: allExtractedData
        });
    } catch (error) {
        if (error.response) {
            // 服务器响应状态码非 2xx
            logger.error(`请求失败，状态码: ${error.response.status}，响应数据: ${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            // 有请求但无响应
            logger.error('请求发送成功，但没有收到响应');
        } else {
            // 设置请求时出错
            logger.error(`请求设置出错: ${error.message}`);
        }
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});    

// 代理接口，用于获取七天单量数据（NY01 仓）
app.post('/proxySevenDaysOrdersNY01', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        // 1. 从customer_info表获取所有符合条件的客户代码
        // 排除DK开头、XXS开头和包含test(不区分大小写)的客户，并按customer_code排序
        const [customers] = await conn.query(
            `SELECT customer_code 
             FROM customer_info 
             WHERE customer_name NOT LIKE 'DK%' 
               AND customer_name NOT LIKE 'XXS%'
               AND LOWER(customer_name) NOT LIKE '%test%'
             ORDER BY customer_code`
        );
        
        // 检查是否有客户数据
        if (customers.length === 0) {
            return res.status(404).json({ 
                message: '未查询到符合条件的客户代码，请先同步客户信息' 
            });
        }
        
        // 提取客户代码并转换为逗号分隔的字符串
        const customerCodes = customers.map(c => c.customer_code).join(',');
        logger.info(`从customer_info获取到${customers.length}个客户代码`);

        // 2. 计算时间范围（最近7天，不包含今天）
        const beijingTime = getBeijingTime(); // 假设已实现北京时区时间获取函数
        const endDate = new Date(beijingTime);
        endDate.setDate(endDate.getDate() - 1); // 昨天
        const endDateStr = formatDate(endDate); // 假设已实现日期格式化函数
        
        const startDate = new Date(endDate);
        startDate.setDate(startDate.getDate() - 6); // 7天前
        const startDateStr = formatDate(startDate);

        // 3. 构造请求数据
        const requestData = {
            "customerCodes": customerCodes, // 使用从数据库获取的客户代码
            "unitMark": 0,
            "whCode": "NY01",
            "timeType": "createTime",
            "startTime": `${startDateStr} 00:00:00`,
            "endTime": `${endDateStr} 23:59:59`,
            "current": 1,
            "size": 20,
            "total": 27826, // 这个值可能需要动态获取或调整
            "exportMode": 0
        };
        
        // 4. 发送请求到外部API
        const response = await instance.post(
            'https://omp.xlwms.com/gateway/omp/order/delivery/export', 
            requestData, 
            {
                headers: config.externalApi.headers,
                timeout: 15000
            }
        );
        
        res.json(response.data);
    } catch (error) {
        // 错误处理
        if (error.response) {
            logger.error(`请求失败，状态码: ${error.response.status}，响应数据: ${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            logger.error('请求发送成功，但没有收到响应');
        } else {
            logger.error(`请求设置出错: ${error.message}`);
        }
        res.status(500).json({ message: '代理请求失败', error: error.message });
    } finally {
        // 确保连接释放
        conn.release();
    }
});
    


// 代理接口，用于获取七天单量数据（CA01 仓）
app.post('/proxySevenDaysOrdersCA01', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        // 从customer_info表获取符合条件的客户代码（排除DK开头、XXS开头和包含test）
        const [customers] = await conn.query(
            `SELECT customer_code 
             FROM customer_info 
             WHERE customer_name NOT LIKE 'DK%' 
               AND customer_name NOT LIKE 'XXS%'
               AND LOWER(customer_name) NOT LIKE '%test%'
             ORDER BY customer_code`
        );
        
        if (customers.length === 0) {
            return res.status(404).json({ 
                message: '未查询到符合条件的客户代码，请先同步客户信息' 
            });
        }
        
        // 转换为逗号分隔的字符串
        const customerCodes = customers.map(c => c.customer_code).join(',');
        logger.info(`CA01仓七天单量接口：获取到${customers.length}个客户代码`);

        // 计算时间范围（最近7天，不含今天）
        const beijingTime = getBeijingTime();
        const endDate = new Date(beijingTime);
        endDate.setDate(endDate.getDate() - 1);
        const endDateStr = formatDate(endDate);
        const startDate = new Date(endDate);
        startDate.setDate(startDate.getDate() - 6);
        const startDateStr = formatDate(startDate);

        // 构造请求数据
        const requestData = {
            "customerCodes": customerCodes, // 动态客户代码
            "unitMark": 0,
            "whCode": "CA01",
            "timeType": "createTime",
            "startTime": `${startDateStr} 00:00:00`,
            "endTime": `${endDateStr} 23:59:59`,
            "current": 1,
            "size": 20,
            "total": 27826, // 若需要动态获取可进一步优化
            "exportMode": 0
        };
        
        // 发送请求
        const response = await instance.post(
            'https://omp.xlwms.com/gateway/omp/order/delivery/export', 
            requestData, 
            {
                headers: config.externalApi.headers,
                timeout: 15000
            }
        );
        
        res.json(response.data);
    } catch (error) {
        // 错误处理
        if (error.response) {
            logger.error(`CA01仓七天单量请求失败：状态码${error.response.status}，响应：${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            logger.error('CA01仓七天单量请求无响应');
        } else {
            logger.error(`CA01仓七天单量请求错误：${error.message}`);
        }
        res.status(500).json({ message: 'CA01仓代理请求失败', error: error.message });
    } finally {
        conn.release(); // 释放数据库连接
    }
});



// 代理接口，用于获取 14 天出库情况数据
app.post('/proxyFourteenDaysOutbound', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        // 从customer_info表获取客户代码（排除DK开头、XXS开头和包含test）
        const [customers] = await conn.query(
            `SELECT customer_code 
             FROM customer_info 
             WHERE customer_name NOT LIKE 'DK%' 
               AND customer_name NOT LIKE 'XXS%'
               AND LOWER(customer_name) NOT LIKE '%test%'
             ORDER BY customer_code`
        );
        
        if (customers.length === 0) {
            return res.status(404).json({ 
                message: '未查询到符合条件的客户代码，请先同步客户信息' 
            });
        }
        
        const customerCodes = customers.map(c => c.customer_code).join(',');
        logger.info(`14天出库接口：获取到${customers.length}个客户代码`);

        // 计算时间范围（最近14天，不含今天）
        const beijingTime = getBeijingTime();
        const endDate = new Date(beijingTime);
        endDate.setDate(endDate.getDate() - 1);
        const endDateStr = formatDate(endDate);
        const startDate = new Date(endDate);
        startDate.setDate(startDate.getDate() - 13);
        const startDateStr = formatDate(startDate);

        // 获取仓库参数（NY01/CA01）
        const warehouse = req.query.warehouse;
        if (!['NY01', 'CA01'].includes(warehouse)) {
            return res.status(400).json({ message: '仓库参数错误，仅支持NY01或CA01' });
        }

        // 构造请求数据（动态客户代码+仓库）
        const requestData = {
            "customerCodes": customerCodes,
            "unitMark": 0,
            "whCode": warehouse,
            "timeType": "outboundTime",
            "startTime": `${startDateStr} 00:00:00`,
            "endTime": `${endDateStr} 23:59:59`,
            "current": 1,
            "size": 20,
            "total": 27826,
            "exportMode": 0
        };
        
        // 发送请求
        const response = await instance.post(
            'https://omp.xlwms.com/gateway/omp/order/delivery/export', 
            requestData, 
            {
                headers: config.externalApi.headers,
                timeout: 15000
            }
        );
        
        res.json(response.data);
    } catch (error) {
        // 错误处理
        if (error.response) {
            logger.error(`14天出库请求失败：状态码${error.response.status}，响应：${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            logger.error('14天出库请求无响应');
        } else {
            logger.error(`14天出库请求错误：${error.message}`);
        }
        res.status(500).json({ message: '14天出库代理请求失败', error: error.message });
    } finally {
        conn.release(); // 释放数据库连接
    }
});


// 新增接口：获取NY01仓入库单情况
app.get('/getNY01InboundOrders', async (req, res) => {
    try {
        const beijingTime = getBeijingTime();
        const prevDate = new Date(beijingTime);
        prevDate.setDate(prevDate.getDate()-1);
        const prevDateStr = formatDate(prevDate);

        const url = `https://omp.xlwms.com/gateway/omp/order/asn/page?whCode=NY01&timeType=createTime&startTime=${prevDateStr}%2000%3A00%3A00&endTime=${prevDateStr}%2023%3A59%3A59&current=1&size=20&total=9`;

        const response = await instance.get(url, {
            headers: config.externalApi.headers,
            timeout: 15000
        });

        if (
            response.data &&
            response.data.data &&
            Array.isArray(response.data.data.records)
        ) {
            const records = response.data.data.records;
            // 过滤掉 cancelTime 不为空的记录
            const filteredRecords = records.filter(record => !record.cancelTime);
            const extractedData = filteredRecords.map(record => ({
                创建日期: record.createTime,
                入库单号: record.sourceNo,
                客户: `${record.customerName}(${record.customerCode})`,
                箱数: record.boxCount
            }));

            res.json({
                success: true,
                data: extractedData
            });
        } else {
            logger.error('响应数据结构不完整，无法获取 records 数组');
            res.status(500).json({
                success: false,
                error: '服务端错误',
                details: '响应数据结构不完整，无法获取所需信息'
            });
        }
    } catch (error) {
        if (error.response) {
            logger.error(`请求失败，状态码: ${error.response.status}，响应数据: ${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            logger.error('请求发送成功，但没有收到响应');
        } else {
            logger.error(`请求设置出错: ${error.message}`);
        }
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});

// 新增接口：获取CA01仓入库单情况
app.get('/getCA01InboundOrders', async (req, res) => {
    try {
        const beijingTime = getBeijingTime();
        const prevDate = new Date(beijingTime);
        prevDate.setDate(prevDate.getDate()-1);
        const prevDateStr = formatDate(prevDate);

        const url = `https://omp.xlwms.com/gateway/omp/order/asn/page?whCode=CA01&timeType=createTime&startTime=${prevDateStr}%2000%3A00%3A00&endTime=${prevDateStr}%2023%3A59%3A59&current=1&size=20&total=9`;

        const response = await instance.get(url, {
            headers: config.externalApi.headers,
            timeout: 15000
        });

        if (
            response.data &&
            response.data.data &&
            Array.isArray(response.data.data.records)
        ) {
            const records = response.data.data.records;
            // 过滤掉 cancelTime 不为空的记录
            const filteredRecords = records.filter(record => !record.cancelTime);
            const extractedData = filteredRecords.map(record => ({
                创建日期: record.createTime,
                入库单号: record.sourceNo,
                客户: `${record.customerName}(${record.customerCode})`,
                箱数: record.boxCount
            }));

            res.json({
                success: true,
                data: extractedData
            });
        } else {
            logger.error('响应数据结构不完整，无法获取 records 数组');
            res.status(500).json({
                success: false,
                error: '服务端错误',
                details: '响应数据结构不完整，无法获取所需信息'
            });
        }
    } catch (error) {
        if (error.response) {
            logger.error(`请求失败，状态码: ${error.response.status}，响应数据: ${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            logger.error('请求发送成功，但没有收到响应');
        } else {
            logger.error(`请求设置出错: ${error.message}`);
        }
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});    
// 新增接口：统计每一天的总单量
app.get('/getDailyTotalOrders', async (req, res) => {
    try {
        const conn = await pool.getConnection();
        const query = `
            SELECT 
                DATE_FORMAT(创建时间, '%Y-%m-%d') AS order_date, 
                COUNT(*) AS total_orders
            FROM 
                aoyu_data
            WHERE 
                客户名称 NOT LIKE 'DK%' 
                AND 客户名称 NOT IN ('TEST', 'testA')
            GROUP BY 
                DATE_FORMAT(创建时间, '%Y-%m-%d')
            ORDER BY 
                order_date;
        `;
        const [rows] = await conn.query(query);
        conn.release();
        res.json({
            success: true,
            data: rows
        });
    } catch (error) {
        logger.error(`统计每日总单量失败: ${error.message}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});

// 新增接口：统计每个客户每天的总单量
app.get('/getDailyTotalOrdersByClient', async (req, res) => {
    try {
        const conn = await pool.getConnection();
        const query = `
            SELECT 
                客户名称,
                DATE_FORMAT(创建时间, '%Y-%m-%d') AS order_date, 
                COUNT(*) AS total_orders
            FROM 
                aoyu_data
            WHERE 
                客户名称 NOT LIKE 'DK%' 
                AND 客户名称 NOT IN ('TEST', 'testA')
            GROUP BY 
                客户名称, DATE_FORMAT(创建时间, '%Y-%m-%d')
            ORDER BY 
                客户名称, order_date;
        `;
        const [rows] = await conn.query(query);
        conn.release();
        res.json({
            success: true,
            data: rows
        });
    } catch (error) {
        logger.error(`统计每个客户每日总单量失败: ${error.message}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});    
// 用于存储所有的 EventSource 连接
const clients = [];

// 新增 /a-progress 接口
app.get('/a-progress', (req, res) => {
    // 设置响应头，以支持 Server-Sent Events
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    // 向客户端发送一个空消息，以保持连接
    res.write(':\n\n');

    // 将客户端连接添加到 clients 数组中
    const client = { res };
    clients.push(client);

    // 当客户端断开连接时，从 clients 数组中移除该连接
    req.on('close', () => {
        const index = clients.indexOf(client);
        if (index!== -1) {
            clients.splice(index, 1);
        }
    });
});

// 定义一个函数，用于向所有客户端发送进度信息
function sendProgress(progress) {
    clients.forEach(client => {
        client.res.write(`data: ${progress}\n\n`);
    });
}
// 用于存储 a 接口传入的开始时间和结束时间
let startTimeFromA = null;
let endTimeFromA = null;
// 原有 a 接口代码
app.get('/a', async (req, res) => {
    try {
        const { startTime, endTime } = req.query;

        // 存储 a 接口传入的时间范围
        startTimeFromA = startTime;
        endTimeFromA = endTime;

        if (!startTime || !endTime) {
            return res.status(400).json({
                success: false,
                error: '缺少 startTime 或 endTime 参数'
            });
        }

        let allExtractedData = [];
        // 先获取第一次数据，用于计算 total
        const firstRequestData = {
            unitMark: 0,
            timeType: "outboundTime",
            startTime: startTime,
            endTime: endTime,
            current: 1,
            size: 200,
            total: 0
        };
        const firstResponse = await instance.post('https://omp.xlwms.com/gateway/omp/order/delivery/page', firstRequestData, {
            headers: config.externalApi.headers,
            timeout: 15000
        });
        const total = firstResponse.data.data.total;
        const pageCount = Math.ceil(total / 200);

        const conn = await pool.getConnection();
        let processedRecords = 0;
        try {
            for (let current = 1; current <= pageCount; current++) {
                const requestData = {
                    unitMark: 0,
                    timeType: "outboundTime",
                    startTime: startTime,
                    endTime: endTime,
                    current: current,
                    size: 200,
                    total: 0
                };
                const response = await instance.post('https://omp.xlwms.com/gateway/omp/order/delivery/page', requestData, {
                    headers: config.externalApi.headers,
                    timeout: 15000
                });

                // 检查 response.data.data.records 是否为数组
                let records = response.data.data.records;
                if (!Array.isArray(records)) {
                    records = records ? [records] : [];
                }

                // 提取所需字段（新增pickTime拣货时间）
                const extractedData = records.map(item => {
                    const { productList, whCodeName, pickTime, ...rest } = item; // 提取pickTime
                    let skuData = '';
                    if (productList && productList.length > 0) {
                        skuData = productList.map(p => `${p.productSku}*${p.qty || ''}`).join('\n');
                    }
                    return {
                        ...rest,
                        仓库: whCodeName,
                        SKU: skuData,
                        客户名称: item.customerName,
                        出库单号: item.sourceNo,
                        物流渠道组: item.channelGroupCode,
                        物流渠道: item.logisticsChannelName,
                        上网时间: item.receiptTime,
                        创建时间: item.createTime,
                        妥投时间: item.deliveredTime,
                        出库时间: item.outboundTime,
                        跟踪单号: item.expressNo,
                        拣货时间: pickTime || '' // 新增拣货时间字段，默认空字符串
                    };
                });
                allExtractedData = allExtractedData.concat(extractedData);

                for (const data of extractedData) {
                    const { 
                        客户名称, 出库单号, 物流渠道组, 物流渠道, 
                        上网时间, 创建时间, 妥投时间, 出库时间, 
                        仓库, SKU, 跟踪单号, 拣货时间 // 解构新增的拣货时间
                    } = data;

                    // 过滤掉客户名称以 DK、test 或 TEST 开头的数据
                    if (
                        data.客户名称.startsWith('DK') ||
                        data.客户名称.toLowerCase().startsWith('test')
                    ) {
                        continue;
                    }

                    // 处理时间字段，如果为空字符串则转换为 null
                    const formatted上网时间 = 上网时间 === '' ? null : 上网时间;
                    const formatted创建时间 = 创建时间 === '' ? null : 创建时间;
                    const formatted妥投时间 = 妥投时间 === '' ? null : 妥投时间;
                    const formatted出库时间 = 出库时间 === '' ? null : 出库时间;
                    // 新增：处理拣货时间，空字符串转为null
                    const formatted拣货时间 = 拣货时间 === '' ? null : 拣货时间;

                    // 插入数据到数据库（新增拣货时间字段）
                    const [result] = await conn.query(`
                        INSERT INTO aoyu_data (
                            客户名称, 出库单号, 物流渠道组, 物流渠道, 
                            创建时间, 出库时间, 上网时间, 妥投时间, 
                            仓库, SKU, 妥投时效, 出库时效, 上网时效, 
                            跟踪单号, 拣货时间  -- 新增拣货时间字段
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)
                        ON DUPLICATE KEY UPDATE
                            客户名称 = VALUES(客户名称),
                            物流渠道组 = VALUES(物流渠道组),
                            物流渠道 = VALUES(物流渠道),
                            创建时间 = VALUES(创建时间),
                            出库时间 = VALUES(出库时间),
                            上网时间 = VALUES(上网时间),
                            妥投时间 = VALUES(妥投时间),
                            仓库 = VALUES(仓库),
                            SKU = VALUES(SKU),
                            跟踪单号 = VALUES(跟踪单号),
                            拣货时间 = VALUES(拣货时间)  -- 新增：更新时同步拣货时间
                    `, [
                        客户名称, 出库单号, 物流渠道组, 物流渠道,
                        formatted创建时间, formatted出库时间, formatted上网时间, formatted妥投时间,
                        仓库, SKU, 跟踪单号, formatted拣货时间  // 新增拣货时间参数
                    ]);

                    processedRecords++;
                    const progress = (processedRecords / total) * 100;
                    console.log(`当前进度: ${progress.toFixed(2)}%`);
                }
            }
            logger.info('数据已成功写入 aoyu_data 表（包含拣货时间）');

            res.json({
                success: true,
                message: '数据已成功写入数据库（包含拣货时间）'
            });
        } catch (dbError) {
            logger.error(`写入 aoyu_data 表时出错: ${dbError.message}`);
            res.status(500).json({
                success: false,
                error: '写入数据库时出错',
                details: dbError.message
            });
            return;
        } finally {
            conn.release();
        }

    } catch (error) {
        if (error.response) {
            logger.error(`请求失败，状态码: ${error.response.status}，响应数据: ${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            logger.error('请求发送成功，但没有收到响应');
        } else {
            logger.error(`请求设置出错: ${error.message}`);
        }
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});
    
// 美国假期列表（2025年）
const usHolidays = [
    new Date(2025, 1, 1),  // 美国新年
    new Date(2025, 7, 4),  // 美国独立日
    new Date(2025, 12, 25), // 圣诞节
    new Date(2025, 5, 26),  // 阵亡将士纪念日
    new Date(2025, 11, 27), // 感恩节
    new Date(2025, 9, 1)    // 劳动节
];

// 检查是否为休息日（周六和周日）
function isWeekend(date) {
    const day = date.getDay();
    return day === 0 || day === 6;
}

// 检查是否为美国节假日
function isUSHoliday(date) {
    const year = date.getFullYear();
    const month = date.getMonth();
    const day = date.getDate();
    return usHolidays.some(holiday => {
        return holiday.getFullYear() === year && holiday.getMonth() === month && holiday.getDate() === day;
    });
}

// 检查是否为美国假期或周末
function isHolidayOrWeekend(date) {
    return isWeekend(date) || isUSHoliday(date);
}

// 将时间转换为美国时间（减去 12 小时）
function convertToUSTime(date) { 
    return new Date(date.getTime() - 12 * 60 * 60 * 1000);
}

// 将时间转换为中国时间（加上 12 小时）
function convertToChinaTime(date) { 
    return new Date(date.getTime() + 12 * 60 * 60 * 1000);
}

// 格式化时间为 2025-03-24 19:08:09 格式
function formatDateTime(date) {
    if (!date) return null;
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

// 处理创建时间，若当天是美国假期或者周末，往后推移到下一个工作日，并将时间设为 00:00:00
function processCreationTime(creationTime) {
    if (!creationTime) return null;
    let newCreationTime = convertToUSTime(creationTime);
    let creationDate = new Date(newCreationTime.getFullYear(), newCreationTime.getMonth(), newCreationTime.getDate());
    if (isHolidayOrWeekend(creationDate)) {
        while (isHolidayOrWeekend(creationDate)) {
            creationDate.setDate(creationDate.getDate() + 1);
        }
        return new Date(creationDate.getFullYear(), creationDate.getMonth(), creationDate.getDate(), 0, 0, 0);
    }
    return newCreationTime;
}
// 处理出库时间，仅返回美国时间
function processOutboundTime(outboundTime) {
    if (!outboundTime) return null;
    return convertToUSTime(outboundTime);
}    

// 处理上网时间，直接返回美国时间
function processOnlineTime(onlineTime) {
    if (!onlineTime) return null;
    return convertToUSTime(onlineTime);
}

// 处理妥投时间，直接返回美国时间
function processDeliveryTime(deliveryTime) {
    if (!deliveryTime) return null;
    return convertToUSTime(deliveryTime);
}    

// 计算两个时间之间的差值（以小时为单位），不去除休息日
function calculateTimeDifferenceInHours(start, end) {
    if (!start ||!end) {
        console.log(`计算时间差时遇到空值，start: ${start}, end: ${end}`);
        return 888;
    }
    return (end - start) / (1000 * 60 * 60);
}

// 计算两个时间之间的差值（以小时为单位），去除休息日
function calculateTimeDifferenceInHoursWithoutHolidays(start, end) {
    let current = new Date(start);
    let totalHours = 0;
    while (current < end) {
        if (!isHolidayOrWeekend(current)) {
            if (current.getDate() === end.getDate()) {
                totalHours += (end - current) / (1000 * 60 * 60);
            } else {
                const endOfDay = new Date(current.getFullYear(), current.getMonth(), current.getDate(), 24, 0, 0);
                totalHours += (endOfDay - current) / (1000 * 60 * 60);
            }
        }
        current.setDate(current.getDate() + 1);
        current.setHours(0, 0, 0, 0);
    }
    return totalHours;
}
    

app.get('/b', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        if (!startTimeFromA ||!endTimeFromA) {
            console.log('未获取到时间范围，无法计算时效信息');
            res.json({
                success: false,
                error: '未获取到时间范围，无法计算时效信息'
            });
            return;
        }

        // 构建 SQL 查询语句，筛选出库时间在指定范围内的数据
        const [rows] = await conn.query(`
            SELECT * 
            FROM aoyu_data 
            WHERE DATE(出库时间) BETWEEN ? AND ?
        `, [startTimeFromA, endTimeFromA]);

        for (const row of rows) {
            const creationTimeStr = row.创建时间;
            const outboundTimeStr = row.出库时间;
            const onlineTimeStr = row.上网时间;
            const deliveryTimeStr = row.妥投时间;

            const creationTime = creationTimeStr? new Date(creationTimeStr) : null;
            const outboundTime = outboundTimeStr? new Date(outboundTimeStr) : null;
            const onlineTime = onlineTimeStr? new Date(onlineTimeStr) : null;
            const deliveryTime = deliveryTimeStr? new Date(deliveryTimeStr) : null;

            const processedCreationTime = processCreationTime(creationTime);
            const processedOutboundTime = processOutboundTime(outboundTime);
            const processedOnlineTime = processOnlineTime(onlineTime);
            const processedDeliveryTime = processDeliveryTime(deliveryTime);

            // 计算出库时效，使用去除周末的函数
            let outboundEfficiency;
            if (!processedCreationTime ||!processedOutboundTime) {
                outboundEfficiency = 888;
            } else {
                outboundEfficiency = calculateTimeDifferenceInHoursWithoutHolidays(processedCreationTime, processedOutboundTime);
            }

            // 计算上网时效
            let onlineEfficiency;
            if (!processedOutboundTime ||!processedOnlineTime) {
                onlineEfficiency = 888;
            } else {
                const chinaOutboundTime = convertToChinaTime(processedOutboundTime);
                const chinaOnlineTime = convertToChinaTime(processedOnlineTime);
                let onlineEfficiencyHours = calculateTimeDifferenceInHours(chinaOutboundTime, chinaOnlineTime);
                onlineEfficiency = onlineEfficiencyHours;
            }

            // 计算妥投时效
            let deliveryEfficiency;
            if (!processedOnlineTime ||!processedDeliveryTime) {
                deliveryEfficiency = 888;
            } else {
                // 使用 calculateTimeDifferenceInHoursWithoutHolidays 函数去除休息日
                let deliveryEfficiencyHours = calculateTimeDifferenceInHoursWithoutHolidays(processedOutboundTime, processedDeliveryTime);
                deliveryEfficiency = deliveryEfficiencyHours / 24;
            }

            // 取整操作
            const intOutboundEfficiency = Math.floor(outboundEfficiency);
            const intOnlineEfficiency = Math.floor(onlineEfficiency / 24);
            const intDeliveryEfficiency = Math.floor(deliveryEfficiency);

            // 更新数据库中的出库时效、上网时效、妥投时效信息
            await conn.query(
                'UPDATE aoyu_data SET 出库时效 = ?, 上网时效 = ?, 妥投时效 = ? WHERE 出库单号 = ?',
                [intOutboundEfficiency, intOnlineEfficiency, intDeliveryEfficiency, row.出库单号]
            );
        }

        console.log('出库时效、上网时效和妥投时效信息已计算并更新到数据库');
        res.json({
            success: true,
            message: '出库时效、上网时效和妥投时效信息已计算并更新到数据库'
        });
    } catch (error) {
        console.error(`计算和更新出库时效、上网时效和妥投时效信息失败: ${error.message}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    } finally {
        conn.release();
    }
});
// 每个客户每日的出库率、妥投率、上网率接口
app.get('/getEveryOneResult', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        // 设置数据库时区为美国东部时间
        await conn.query("SET time_zone = '-05:00'"); 

        // 从请求参数中获取筛选条件
        const { customerName, startDate, endDate } = req.query;

        // 构建筛选条件字符串
        let filterConditions = [];
        if (customerName) {
            filterConditions.push(`客户名称 = '${customerName}'`);
        }
        if (startDate && endDate) {
            filterConditions.push(`创建时间 BETWEEN '${startDate}' AND '${endDate}'`);
        }
        // 原有的物流渠道和客户名称筛选条件
        filterConditions.push(`物流渠道 NOT LIKE '%上传物流面单%'`);
        filterConditions.push(`客户名称 NOT LIKE 'DK%' AND 客户名称 NOT IN ('TEST', 'testA')`);

        const filterConditionString = filterConditions.length > 0 ? `WHERE ${filterConditions.join(' AND ')}` : '';

        const customerQuery = `
            SELECT 
                客户名称,
                DATE_FORMAT(创建时间, '%Y-%m-%d') AS creation_date,
                DATE_FORMAT(出库时间, '%Y-%m-%d') AS outbound_date,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN 上网时效 <= 1 THEN 1 ELSE 0 END) AS within_24h_online_count,
                SUM(CASE WHEN 上网时效 <= 2 THEN 1 ELSE 0 END) AS within_48h_online_count,
                SUM(CASE WHEN 出库时效 <= 24 THEN 1 ELSE 0 END) AS within_24h_outbound_count,
                SUM(CASE WHEN 出库时效 <= 48 THEN 1 ELSE 0 END) AS within_48h_outbound_count,
                SUM(CASE WHEN 妥投时效 <= 3 THEN 1 ELSE 0 END) AS within_3days_delivery_count,
                SUM(CASE WHEN 妥投时效 <= 5 THEN 1 ELSE 0 END) AS within_5days_delivery_count,
                SUM(CASE WHEN 妥投时效 <= 7 THEN 1 ELSE 0 END) AS within_7days_delivery_count,
                SUM(CASE WHEN 妥投时效 > 7 THEN 1 ELSE 0 END) AS over_7days_delivery_count
            FROM 
                aoyu_data
            ${filterConditionString}
            GROUP BY 
                客户名称, DATE_FORMAT(创建时间, '%Y-%m-%d'), DATE_FORMAT(出库时间, '%Y-%m-%d')
            ORDER BY 
                客户名称, creation_date, outbound_date;
        `;
        const [customerRows] = await conn.query(customerQuery);

        const customerResult = customerRows.map(row => {
            const totalOrders = row.total_orders;
            return {
                customer: row.客户名称,
                creation_date: row.creation_date,
                outbound_date: row.outbound_date,
                creation_time_rates: {
                    '24小时出库率': totalOrders > 0? (row.within_24h_outbound_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '48小时出库率': totalOrders > 0? (row.within_48h_outbound_count / totalOrders * 100).toFixed(2) + '%' : '0%'
                },
                outbound_time_rates: {
                    '24小时上网率': totalOrders > 0? (row.within_24h_online_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '48小时上网率': totalOrders > 0? (row.within_48h_online_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '3天妥投率': totalOrders > 0? (row.within_3days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '5天妥投率': totalOrders > 0? (row.within_5days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '7天妥投率': totalOrders > 0? (row.within_7days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '异常妥投率': totalOrders > 0? (row.over_7days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%'
                }
            };
        });

        res.json({
            success: true,
            data: customerResult
        });
    } catch (error) {
        console.error(`获取每个客户每天的上网率、妥投率、出库率失败: ${error.message}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    } finally {
        conn.release();
    }
});
    
// 每日总（所有客户）的出库率、妥投率、上网率接口（按渠道分组）
app.get('/getALLResultByChannel', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        // 设置数据库时区为美国东部时间
        await conn.query("SET time_zone = '-05:00'"); 

        // 从请求参数中获取筛选条件
        const { customerName, startDate, endDate } = req.query;

        // 构建基础筛选条件
        let baseFilterConditions = [];
        if (customerName) {
            baseFilterConditions.push(`客户名称 = '${customerName}'`);
        }
        if (startDate && endDate) {
            baseFilterConditions.push(`创建时间 BETWEEN '${startDate}' AND '${endDate}'`);
        }
        baseFilterConditions.push(`客户名称 NOT LIKE 'DK%' AND 客户名称 NOT IN ('TEST', 'testA')`);
        // 添加仓库筛选条件
        baseFilterConditions.push(`仓库 = 'NY01仓'`);

        const baseFilterConditionString = baseFilterConditions.length > 0 ? `WHERE ${baseFilterConditions.join(' AND ')}` : '';
        // 新增：统计每个客户每天的总单量
        const customerDailyOrdersQuery = `
            SELECT 
                客户名称 AS customer_name,
                DATE_FORMAT(创建时间, '%Y-%m-%d') AS date,
                COUNT(*) AS total_orders,
                物流渠道 AS logistics_channel
            FROM 
                aoyu_data
            WHERE 
                仓库 = 'NY01仓' AND
                客户名称 NOT LIKE 'DK%' AND 
                客户名称 NOT IN ('TEST', 'testA')
            GROUP BY 
                客户名称, DATE_FORMAT(创建时间, '%Y-%m-%d'), 物流渠道
            ORDER BY 
                客户名称, date;
        `;
        const [customerDailyOrdersRows] = await conn.query(customerDailyOrdersQuery);
        // 新增：统计所有客户每天的总单量
        const allCustomersDailyOrdersQuery = `
            SELECT 
                DATE_FORMAT(创建时间, '%Y-%m-%d') AS date,
                COUNT(*) AS total_orders
            FROM 
                aoyu_data
            ${baseFilterConditionString}
            GROUP BY 
                DATE_FORMAT(创建时间, '%Y-%m-%d')
            ORDER BY 
                date;
        `;
        const [allCustomersDailyOrdersRows] = await conn.query(allCustomersDailyOrdersQuery);
        // 统计出库率，根据创建时间和物流渠道分组
        const outboundRateQuery = `
            SELECT 
                物流渠道,
                DATE_FORMAT(创建时间, '%Y-%m-%d') AS creation_date,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN 出库时效 <= 24 THEN 1 ELSE 0 END) AS within_24h_outbound_count,
                SUM(CASE WHEN 出库时效 <= 48 THEN 1 ELSE 0 END) AS within_48h_outbound_count
            FROM 
                aoyu_data
            ${baseFilterConditionString}
            GROUP BY 
                物流渠道, DATE_FORMAT(创建时间, '%Y-%m-%d')
            ORDER BY 
                物流渠道, creation_date;
        `;
        const [outboundRateRows] = await conn.query(outboundRateQuery);

        // 统计上网率和妥投率，根据出库时间和物流渠道分组
        const onlineAndDeliveryRateQuery = `
            SELECT 
                物流渠道,
                DATE_FORMAT(出库时间, '%Y-%m-%d') AS outbound_date,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN 上网时效 <= 1 THEN 1 ELSE 0 END) AS within_24h_online_count,
                SUM(CASE WHEN 上网时效 <= 2 THEN 1 ELSE 0 END) AS within_48h_online_count,
                SUM(CASE WHEN 妥投时效 <= 3 THEN 1 ELSE 0 END) AS within_3days_delivery_count,
                SUM(CASE WHEN 妥投时效 <= 5 THEN 1 ELSE 0 END) AS within_5days_delivery_count,
                SUM(CASE WHEN 妥投时效 <= 7 THEN 1 ELSE 0 END) AS within_7days_delivery_count,
                SUM(CASE WHEN 妥投时效 > 7 THEN 1 ELSE 0 END) AS over_7days_delivery_count
            FROM 
                aoyu_data
            ${baseFilterConditionString}
            GROUP BY 
                物流渠道, DATE_FORMAT(出库时间, '%Y-%m-%d')
            ORDER BY 
                物流渠道, outbound_date;
        `;
        const [onlineAndDeliveryRateRows] = await conn.query(onlineAndDeliveryRateQuery);

        // 按渠道分组处理结果
        const channelResults = {};
        
        // 处理出库率数据
        outboundRateRows.forEach(row => {
            const channel = row.物流渠道;
            if (!channelResults[channel]) {
                channelResults[channel] = {};
            }
            
            const totalOrders = row.total_orders;
            channelResults[channel][row.creation_date] = {
                creation_date: row.creation_date,
                creation_time_rates: {
                    '24小时出库率': totalOrders > 0 ? (row.within_24h_outbound_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '48小时出库率': totalOrders > 0 ? (row.within_48h_outbound_count / totalOrders * 100).toFixed(2) + '%' : '0%'
                }
            };
        });

        // 处理上网率和妥投率数据
        onlineAndDeliveryRateRows.forEach(row => {
            const channel = row.物流渠道;
            if (!channelResults[channel]) {
                channelResults[channel] = {};
            }
            
            const totalOrders = row.total_orders;
            if (channelResults[channel][row.outbound_date]) {
                channelResults[channel][row.outbound_date].outbound_time_rates = {
                    '24小时上网率': totalOrders > 0 ? (row.within_24h_online_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '48小时上网率': totalOrders > 0 ? (row.within_48h_online_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '3天妥投率': totalOrders > 0 ? (row.within_3days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '5天妥投率': totalOrders > 0 ? (row.within_5days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '7天妥投率': totalOrders > 0 ? (row.within_7days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '异常妥投率': totalOrders > 0 ? (row.over_7days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%'
                };
            } else {
                channelResults[channel][row.outbound_date] = {
                    outbound_date: row.outbound_date,
                    outbound_time_rates: {
                        '24小时上网率': totalOrders > 0 ? (row.within_24h_online_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                        '48小时上网率': totalOrders > 0 ? (row.within_48h_online_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                        '3天妥投率': totalOrders > 0 ? (row.within_3days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                        '5天妥投率': totalOrders > 0 ? (row.within_5days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                        '7天妥投率': totalOrders > 0 ? (row.within_7days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                        '异常妥投率': totalOrders > 0 ? (row.over_7days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%'
                    }
                };
            }
        });

        // 格式化最终结果
        const finalResult = [];
        for (const channel in channelResults) {
            const channelData = {
                channel: channel,
                daily_stats: [],
                customer_daily_orders: customerDailyOrdersRows
                    .filter(row => row.logistics_channel === channel)
                    .map(row => ({
                        customer_name: row.customer_name,
                        date: row.date,
                        total_orders: row.total_orders
                    })),
                all_customers_daily_orders: allCustomersDailyOrdersRows
            };
            
            for (const date in channelResults[channel]) {
                channelData.daily_stats.push({
                    ...channelResults[channel][date]
                });
            }
            
            finalResult.push(channelData);
        }

        res.json({
            success: true,
            data: finalResult
        });
    } catch (error) {
        console.error(`获取每天的上网率、妥投率、出库率失败: ${error.message}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    } finally {
        conn.release();
    }
});
// 每日总（所有客户）的出库率、妥投率、上网率接口（按渠道分组）
app.get('/getALLResultByChannelForCA01', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        // 设置数据库时区为美国东部时间
        await conn.query("SET time_zone = '-05:00'"); 

        // 从请求参数中获取筛选条件
        const { customerName, startDate, endDate } = req.query;

        // 构建基础筛选条件
        let baseFilterConditions = [];
        if (customerName) {
            baseFilterConditions.push(`客户名称 = '${customerName}'`);
        }
        if (startDate && endDate) {
            baseFilterConditions.push(`创建时间 BETWEEN '${startDate}' AND '${endDate}'`);
        }
        baseFilterConditions.push(`客户名称 NOT LIKE 'DK%' AND 客户名称 NOT IN ('TEST', 'testA')`);
        // 添加仓库筛选条件
        baseFilterConditions.push(`仓库 = 'CA01'`);

        const baseFilterConditionString = baseFilterConditions.length > 0 ? `WHERE ${baseFilterConditions.join(' AND ')}` : '';
        // 新增：统计每个客户每天的总单量
        const customerDailyOrdersQuery = `
            SELECT 
                客户名称 AS customer_name,
                DATE_FORMAT(创建时间, '%Y-%m-%d') AS date,
                COUNT(*) AS total_orders,
                物流渠道 AS logistics_channel
            FROM 
                aoyu_data
            WHERE 
                仓库 = 'CA01' AND
                客户名称 NOT LIKE 'DK%' AND 
                客户名称 NOT IN ('TEST', 'testA')
            GROUP BY 
                客户名称, DATE_FORMAT(创建时间, '%Y-%m-%d'), 物流渠道
            ORDER BY 
                客户名称, date;
        `;
        const [customerDailyOrdersRows] = await conn.query(customerDailyOrdersQuery);

        // 新增：统计所有客户每天的总单量
        const allCustomersDailyOrdersQuery = `
            SELECT 
                DATE_FORMAT(创建时间, '%Y-%m-%d') AS date,
                COUNT(*) AS total_orders
            FROM 
                aoyu_data
            ${baseFilterConditionString}
            GROUP BY 
                DATE_FORMAT(创建时间, '%Y-%m-%d')
            ORDER BY 
                date;
        `;
        const [allCustomersDailyOrdersRows] = await conn.query(allCustomersDailyOrdersQuery);
        // 统计出库率，根据创建时间和物流渠道分组
        const outboundRateQuery = `
            SELECT 
                物流渠道,
                DATE_FORMAT(创建时间, '%Y-%m-%d') AS creation_date,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN 出库时效 <= 24 THEN 1 ELSE 0 END) AS within_24h_outbound_count,
                SUM(CASE WHEN 出库时效 <= 48 THEN 1 ELSE 0 END) AS within_48h_outbound_count
            FROM 
                aoyu_data
            ${baseFilterConditionString}
            GROUP BY 
                物流渠道, DATE_FORMAT(创建时间, '%Y-%m-%d')
            ORDER BY 
                物流渠道, creation_date;
        `;
        const [outboundRateRows] = await conn.query(outboundRateQuery);

        // 统计上网率和妥投率，根据出库时间和物流渠道分组
        const onlineAndDeliveryRateQuery = `
            SELECT 
                物流渠道,
                DATE_FORMAT(出库时间, '%Y-%m-%d') AS outbound_date,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN 上网时效 <= 1 THEN 1 ELSE 0 END) AS within_24h_online_count,
                SUM(CASE WHEN 上网时效 <= 2 THEN 1 ELSE 0 END) AS within_48h_online_count,
                SUM(CASE WHEN 妥投时效 <= 3 THEN 1 ELSE 0 END) AS within_3days_delivery_count,
                SUM(CASE WHEN 妥投时效 <= 5 THEN 1 ELSE 0 END) AS within_5days_delivery_count,
                SUM(CASE WHEN 妥投时效 <= 7 THEN 1 ELSE 0 END) AS within_7days_delivery_count,
                SUM(CASE WHEN 妥投时效 > 7 THEN 1 ELSE 0 END) AS over_7days_delivery_count
            FROM 
                aoyu_data
            ${baseFilterConditionString}
            GROUP BY 
                物流渠道, DATE_FORMAT(出库时间, '%Y-%m-%d')
            ORDER BY 
                物流渠道, outbound_date;
        `;
        const [onlineAndDeliveryRateRows] = await conn.query(onlineAndDeliveryRateQuery);

        // 按渠道分组处理结果
        const channelResults = {};
        
        // 处理出库率数据
        outboundRateRows.forEach(row => {
            const channel = row.物流渠道;
            if (!channelResults[channel]) {
                channelResults[channel] = {};
            }
            
            const totalOrders = row.total_orders;
            channelResults[channel][row.creation_date] = {
                creation_date: row.creation_date,
                creation_time_rates: {
                    '24小时出库率': totalOrders > 0 ? (row.within_24h_outbound_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '48小时出库率': totalOrders > 0 ? (row.within_48h_outbound_count / totalOrders * 100).toFixed(2) + '%' : '0%'
                }
            };
        });

        // 处理上网率和妥投率数据
        onlineAndDeliveryRateRows.forEach(row => {
            const channel = row.物流渠道;
            if (!channelResults[channel]) {
                channelResults[channel] = {};
            }
            
            const totalOrders = row.total_orders;
            if (channelResults[channel][row.outbound_date]) {
                channelResults[channel][row.outbound_date].outbound_time_rates = {
                    '24小时上网率': totalOrders > 0 ? (row.within_24h_online_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '48小时上网率': totalOrders > 0 ? (row.within_48h_online_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '3天妥投率': totalOrders > 0 ? (row.within_3days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '5天妥投率': totalOrders > 0 ? (row.within_5days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '7天妥投率': totalOrders > 0 ? (row.within_7days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '异常妥投率': totalOrders > 0 ? (row.over_7days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%'
                };
            } else {
                channelResults[channel][row.outbound_date] = {
                    outbound_date: row.outbound_date,
                    outbound_time_rates: {
                        '24小时上网率': totalOrders > 0 ? (row.within_24h_online_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                        '48小时上网率': totalOrders > 0 ? (row.within_48h_online_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                        '3天妥投率': totalOrders > 0 ? (row.within_3days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                        '5天妥投率': totalOrders > 0 ? (row.within_5days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                        '7天妥投率': totalOrders > 0 ? (row.within_7days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                        '异常妥投率': totalOrders > 0 ? (row.over_7days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%'
                    }
                };
            }
        });

        // 格式化最终结果
        const finalResult = [];
        for (const channel in channelResults) {
            const channelData = {
                channel: channel,
                daily_stats: [],
                customer_daily_orders: customerDailyOrdersRows
                    .filter(row => row.logistics_channel === channel)
                    .map(row => ({
                        customer_name: row.customer_name,
                        date: row.date,
                        total_orders: row.total_orders
                    })),
                all_customers_daily_orders: allCustomersDailyOrdersRows
            };
            
            for (const date in channelResults[channel]) {
                channelData.daily_stats.push({
                    ...channelResults[channel][date]
                });
            }
            
            finalResult.push(channelData);
        }

        res.json({
            success: true,
            data: finalResult
        });
    } catch (error) {
        console.error(`获取每天的上网率、妥投率、出库率失败: ${error.message}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    } finally {
        conn.release();
    }
});
// 每日总（所有客户）的出库率、妥投率、上网率接口（仓库为 CA01）
app.get('/getALLResultForCA01', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        // 设置数据库时区为美国东部时间
        await conn.query("SET time_zone = '-05:00'"); 

        // 从请求参数中获取筛选条件
        const { customerName, startDate, endDate } = req.query;

        // 构建基础筛选条件（不包含渠道过滤）
        let baseFilterConditions = [];
        if (customerName) {
            baseFilterConditions.push(`客户名称 = '${customerName}'`);
        }
        if (startDate && endDate) {
            baseFilterConditions.push(`创建时间 BETWEEN '${startDate}' AND '${endDate}'`);
        }
        baseFilterConditions.push(`客户名称 NOT LIKE 'DK%' AND 客户名称 NOT IN ('TEST', 'testA')`);
        // 添加仓库筛选条件
        baseFilterConditions.push(`仓库 = 'CA01'`);

        const baseFilterConditionString = baseFilterConditions.length > 0 ? `WHERE ${baseFilterConditions.join(' AND ')}` : '';

        // 上网率和妥投率不筛选上传物流面单，使用基础筛选条件
        const onlineDeliveryFilterConditionString = baseFilterConditionString;

        // 统计出库率，根据创建时间分组（使用基础筛选条件）
        const outboundRateQuery = `
            SELECT 
                DATE_FORMAT(创建时间, '%Y-%m-%d') AS creation_date,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN 出库时效 <= 24 THEN 1 ELSE 0 END) AS within_24h_outbound_count,
                SUM(CASE WHEN 出库时效 <= 48 THEN 1 ELSE 0 END) AS within_48h_outbound_count
            FROM 
                aoyu_data
            ${baseFilterConditionString}
            GROUP BY 
                DATE_FORMAT(创建时间, '%Y-%m-%d')
            ORDER BY 
                creation_date;
        `;
        const [outboundRateRows] = await conn.query(outboundRateQuery);

        // 统计上网率和妥投率，根据出库时间分组（使用基础筛选条件，不排除上传物流面单渠道）
        const onlineAndDeliveryRateQuery = `
            SELECT 
                DATE_FORMAT(出库时间, '%Y-%m-%d') AS outbound_date,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN 上网时效 <= 1 THEN 1 ELSE 0 END) AS within_24h_online_count,
                SUM(CASE WHEN 上网时效 <= 2 THEN 1 ELSE 0 END) AS within_48h_online_count,
                SUM(CASE WHEN 妥投时效 <= 3 THEN 1 ELSE 0 END) AS within_3days_delivery_count,
                SUM(CASE WHEN 妥投时效 <= 5 THEN 1 ELSE 0 END) AS within_5days_delivery_count,
                SUM(CASE WHEN 妥投时效 <= 7 THEN 1 ELSE 0 END) AS within_7days_delivery_count,
                SUM(CASE WHEN 妥投时效 > 7 THEN 1 ELSE 0 END) AS over_7days_delivery_count
            FROM 
                aoyu_data
            ${onlineDeliveryFilterConditionString}
            GROUP BY 
                DATE_FORMAT(出库时间, '%Y-%m-%d')
            ORDER BY 
                outbound_date;
        `;
        const [onlineAndDeliveryRateRows] = await conn.query(onlineAndDeliveryRateQuery);

        // 合并两个结果
        const totalResult = [];
        outboundRateRows.forEach(outboundRateItem => {
            const found = onlineAndDeliveryRateRows.find(item => item.outbound_date === outboundRateItem.creation_date);
            if (found) {
                totalResult.push({
                    creation_date: outboundRateItem.creation_date,
                    creation_time_rates: {
                        '24小时出库率': outboundRateItem.total_orders > 0? (outboundRateItem.within_24h_outbound_count / outboundRateItem.total_orders * 100).toFixed(2) + '%' : '0%',
                        '48小时出库率': outboundRateItem.total_orders > 0? (outboundRateItem.within_48h_outbound_count / outboundRateItem.total_orders * 100).toFixed(2) + '%' : '0%'
                    },
                    outbound_date: found.outbound_date,
                    outbound_time_rates: {
                        '24小时上网率': found.total_orders > 0? (found.within_24h_online_count / found.total_orders * 100).toFixed(2) + '%' : '0%',
                        '48小时上网率': found.total_orders > 0? (found.within_48h_online_count / found.total_orders * 100).toFixed(2) + '%' : '0%',
                        '3天妥投率': found.total_orders > 0? (found.within_3days_delivery_count / found.total_orders * 100).toFixed(2) + '%' : '0%',
                        '5天妥投率': found.total_orders > 0? (found.within_5days_delivery_count / found.total_orders * 100).toFixed(2) + '%' : '0%',
                        '7天妥投率': found.total_orders > 0? (found.within_7days_delivery_count / found.total_orders * 100).toFixed(2) + '%' : '0%',
                        '异常妥投率': found.total_orders > 0? (found.over_7days_delivery_count / found.total_orders * 100).toFixed(2) + '%' : '0%'
                    }
                });
            } else {
                totalResult.push({
                    creation_date: outboundRateItem.creation_date,
                    creation_time_rates: {
                        '24小时出库率': outboundRateItem.total_orders > 0? (outboundRateItem.within_24h_outbound_count / outboundRateItem.total_orders * 100).toFixed(2) + '%' : '0%',
                        '48小时出库率': outboundRateItem.total_orders > 0? (outboundRateItem.within_48h_outbound_count / outboundRateItem.total_orders * 100).toFixed(2) + '%' : '0%'
                    },
                    outbound_date: outboundRateItem.creation_date,
                    outbound_time_rates: {
                        '24小时上网率': '0%',
                        '48小时上网率': '0%',
                        '3天妥投率': '0%',
                        '5天妥投率': '0%',
                        '7天妥投率': '0%',
                        '异常妥投率': '0%'
                    }
                });
            }
        });

        // 添加只有上网率和妥投率数据但没有出库率数据的日期
        onlineAndDeliveryRateRows.forEach(onlineAndDeliveryRateItem => {
            const found = totalResult.find(item => item.outbound_date === onlineAndDeliveryRateItem.outbound_date);
            if (!found) {
                totalResult.push({
                    creation_date: onlineAndDeliveryRateItem.outbound_date,
                    creation_time_rates: {
                        '24小时出库率': '0%',
                        '48小时出库率': '0%'
                    },
                    outbound_date: onlineAndDeliveryRateItem.outbound_date,
                    outbound_time_rates: {
                        '24小时上网率': onlineAndDeliveryRateItem.total_orders > 0? (onlineAndDeliveryRateItem.within_24h_online_count / onlineAndDeliveryRateItem.total_orders * 100).toFixed(2) + '%' : '0%',
                        '48小时上网率': onlineAndDeliveryRateItem.total_orders > 0? (onlineAndDeliveryRateItem.within_48h_online_count / onlineAndDeliveryRateItem.total_orders * 100).toFixed(2) + '%' : '0%',
                        '3天妥投率': onlineAndDeliveryRateItem.total_orders > 0? (onlineAndDeliveryRateItem.within_3days_delivery_count / onlineAndDeliveryRateItem.total_orders * 100).toFixed(2) + '%' : '0%',
                        '5天妥投率': onlineAndDeliveryRateItem.total_orders > 0? (onlineAndDeliveryRateItem.within_5days_delivery_count / onlineAndDeliveryRateItem.total_orders * 100).toFixed(2) + '%' : '0%',
                        '7天妥投率': onlineAndDeliveryRateItem.total_orders > 0? (onlineAndDeliveryRateItem.within_7days_delivery_count / onlineAndDeliveryRateItem.total_orders * 100).toFixed(2) + '%' : '0%',
                        '异常妥投率': onlineAndDeliveryRateItem.total_orders > 0? (onlineAndDeliveryRateItem.over_7days_delivery_count / onlineAndDeliveryRateItem.total_orders * 100).toFixed(2) + '%' : '0%'
                    }
                });
            }
        });

        res.json({
            success: true,
            data: totalResult
        });
    } catch (error) {
        console.error(`获取每天的上网率、妥投率、出库率失败: ${error.message}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    } finally {
        conn.release();
    }
});
// 每日总（所有客户）的出库率、妥投率、上网率接口
app.get('/getALLResult', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        // 设置数据库时区为美国东部时间
        await conn.query("SET time_zone = '-05:00'"); 

        // 从请求参数中获取筛选条件
        const { customerName, startDate, endDate } = req.query;

        // 构建基础筛选条件（不包含渠道过滤）
        let baseFilterConditions = [];
        if (customerName) {
            baseFilterConditions.push(`客户名称 = '${customerName}'`);
        }
        if (startDate && endDate) {
            baseFilterConditions.push(`创建时间 BETWEEN '${startDate}' AND '${endDate}'`);
        }
        baseFilterConditions.push(`客户名称 NOT LIKE 'DK%' AND 客户名称 NOT IN ('TEST', 'testA')`);
        // 添加仓库筛选条件
        baseFilterConditions.push(`仓库 = 'NY01仓'`);

        const baseFilterConditionString = baseFilterConditions.length > 0 ? `WHERE ${baseFilterConditions.join(' AND ')}` : '';

        // 构建上网率和妥投率的筛选条件（包含渠道过滤）
        let onlineDeliveryFilterConditions = [...baseFilterConditions];
        onlineDeliveryFilterConditions.push(`物流渠道 NOT LIKE '%上传物流面单%'`);
        const onlineDeliveryFilterConditionString = onlineDeliveryFilterConditions.length > 0 ? `WHERE ${onlineDeliveryFilterConditions.join(' AND ')}` : '';

        // 统计出库率，根据创建时间分组（使用基础筛选条件）
        const outboundRateQuery = `
            SELECT 
                DATE_FORMAT(创建时间, '%Y-%m-%d') AS creation_date,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN 出库时效 <= 24 THEN 1 ELSE 0 END) AS within_24h_outbound_count,
                SUM(CASE WHEN 出库时效 <= 48 THEN 1 ELSE 0 END) AS within_48h_outbound_count
            FROM 
                aoyu_data
            ${baseFilterConditionString}
            GROUP BY 
                DATE_FORMAT(创建时间, '%Y-%m-%d')
            ORDER BY 
                creation_date;
        `;
        const [outboundRateRows] = await conn.query(outboundRateQuery);

        // 统计上网率和妥投率，根据出库时间分组（使用包含渠道过滤的条件）
        const onlineAndDeliveryRateQuery = `
            SELECT 
                DATE_FORMAT(出库时间, '%Y-%m-%d') AS outbound_date,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN 上网时效 <= 1 THEN 1 ELSE 0 END) AS within_24h_online_count,
                SUM(CASE WHEN 上网时效 <= 2 THEN 1 ELSE 0 END) AS within_48h_online_count,
                SUM(CASE WHEN 妥投时效 <= 3 THEN 1 ELSE 0 END) AS within_3days_delivery_count,
                SUM(CASE WHEN 妥投时效 <= 5 THEN 1 ELSE 0 END) AS within_5days_delivery_count,
                SUM(CASE WHEN 妥投时效 <= 7 THEN 1 ELSE 0 END) AS within_7days_delivery_count,
                SUM(CASE WHEN 妥投时效 > 7 THEN 1 ELSE 0 END) AS over_7days_delivery_count
            FROM 
                aoyu_data
            ${onlineDeliveryFilterConditionString}
            GROUP BY 
                DATE_FORMAT(出库时间, '%Y-%m-%d')
            ORDER BY 
                outbound_date;
        `;
        const [onlineAndDeliveryRateRows] = await conn.query(onlineAndDeliveryRateQuery);

        // 合并两个结果
        const totalResult = [];
        outboundRateRows.forEach(outboundRateItem => {
            const found = onlineAndDeliveryRateRows.find(item => item.outbound_date === outboundRateItem.creation_date);
            if (found) {
                totalResult.push({
                    creation_date: outboundRateItem.creation_date,
                    creation_time_rates: {
                        '24小时出库率': outboundRateItem.total_orders > 0? (outboundRateItem.within_24h_outbound_count / outboundRateItem.total_orders * 100).toFixed(2) + '%' : '0%',
                        '48小时出库率': outboundRateItem.total_orders > 0? (outboundRateItem.within_48h_outbound_count / outboundRateItem.total_orders * 100).toFixed(2) + '%' : '0%'
                    },
                    outbound_date: found.outbound_date,
                    outbound_time_rates: {
                        '24小时上网率': found.total_orders > 0? (found.within_24h_online_count / found.total_orders * 100).toFixed(2) + '%' : '0%',
                        '48小时上网率': found.total_orders > 0? (found.within_48h_online_count / found.total_orders * 100).toFixed(2) + '%' : '0%',
                        '3天妥投率': found.total_orders > 0? (found.within_3days_delivery_count / found.total_orders * 100).toFixed(2) + '%' : '0%',
                        '5天妥投率': found.total_orders > 0? (found.within_5days_delivery_count / found.total_orders * 100).toFixed(2) + '%' : '0%',
                        '7天妥投率': found.total_orders > 0? (found.within_7days_delivery_count / found.total_orders * 100).toFixed(2) + '%' : '0%',
                        '异常妥投率': found.total_orders > 0? (found.over_7days_delivery_count / found.total_orders * 100).toFixed(2) + '%' : '0%'
                    }
                });
            } else {
                totalResult.push({
                    creation_date: outboundRateItem.creation_date,
                    creation_time_rates: {
                        '24小时出库率': outboundRateItem.total_orders > 0? (outboundRateItem.within_24h_outbound_count / outboundRateItem.total_orders * 100).toFixed(2) + '%' : '0%',
                        '48小时出库率': outboundRateItem.total_orders > 0? (outboundRateItem.within_48h_outbound_count / outboundRateItem.total_orders * 100).toFixed(2) + '%' : '0%'
                    },
                    outbound_date: outboundRateItem.creation_date,
                    outbound_time_rates: {
                        '24小时上网率': '0%',
                        '48小时上网率': '0%',
                        '3天妥投率': '0%',
                        '5天妥投率': '0%',
                        '7天妥投率': '0%',
                        '异常妥投率': '0%'
                    }
                });
            }
        });

        // 添加只有上网率和妥投率数据但没有出库率数据的日期
        onlineAndDeliveryRateRows.forEach(onlineAndDeliveryRateItem => {
            const found = totalResult.find(item => item.outbound_date === onlineAndDeliveryRateItem.outbound_date);
            if (!found) {
                totalResult.push({
                    creation_date: onlineAndDeliveryRateItem.outbound_date,
                    creation_time_rates: {
                        '24小时出库率': '0%',
                        '48小时出库率': '0%'
                    },
                    outbound_date: onlineAndDeliveryRateItem.outbound_date,
                    outbound_time_rates: {
                        '24小时上网率': onlineAndDeliveryRateItem.total_orders > 0? (onlineAndDeliveryRateItem.within_24h_online_count / onlineAndDeliveryRateItem.total_orders * 100).toFixed(2) + '%' : '0%',
                        '48小时上网率': onlineAndDeliveryRateItem.total_orders > 0? (onlineAndDeliveryRateItem.within_48h_online_count / onlineAndDeliveryRateItem.total_orders * 100).toFixed(2) + '%' : '0%',
                        '3天妥投率': onlineAndDeliveryRateItem.total_orders > 0? (onlineAndDeliveryRateItem.within_3days_delivery_count / onlineAndDeliveryRateItem.total_orders * 100).toFixed(2) + '%' : '0%',
                        '5天妥投率': onlineAndDeliveryRateItem.total_orders > 0? (onlineAndDeliveryRateItem.within_5days_delivery_count / onlineAndDeliveryRateItem.total_orders * 100).toFixed(2) + '%' : '0%',
                        '7天妥投率': onlineAndDeliveryRateItem.total_orders > 0? (onlineAndDeliveryRateItem.within_7days_delivery_count / onlineAndDeliveryRateItem.total_orders * 100).toFixed(2) + '%' : '0%',
                        '异常妥投率': onlineAndDeliveryRateItem.total_orders > 0? (onlineAndDeliveryRateItem.over_7days_delivery_count / onlineAndDeliveryRateItem.total_orders * 100).toFixed(2) + '%' : '0%'
                    }
                });
            }
        });

        res.json({
            success: true,
            data: totalResult
        });
    } catch (error) {
        console.error(`获取每天的上网率、妥投率、出库率失败: ${error.message}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    } finally {
        conn.release();
    }
});    
// 新增接口：导出 aoyu_data 表的数据为 XLSX 文件
app.get('/c', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        // 每页查询的记录数
        const pageSize = 1000; 
        let page = 1;
        let allRows = [];

        while (true) {
            const offset = (page - 1) * pageSize;
            const query = `SELECT * FROM aoyu_data LIMIT ${offset}, ${pageSize}`;
            const [rows] = await conn.query(query);

            if (rows.length === 0) {
                break;
            }

            allRows = allRows.concat(rows);
            page++;
        }

        // 格式化日期字段
        const formattedRows = allRows.map(row => {
            const newRow = { ...row };
            const dateFields = ['创建时间', '出库时间', '上网时间', '妥投时间'];
            dateFields.forEach(field => {
                if (newRow[field]) {
                    const date = new Date(newRow[field]);
                    const year = date.getFullYear();
                    const month = String(date.getMonth() + 1).padStart(2, '0');
                    const day = String(date.getDate()).padStart(2, '0');
                    const hours = String(date.getHours()).padStart(2, '0');
                    const minutes = String(date.getMinutes()).padStart(2, '0');
                    const seconds = String(date.getSeconds()).padStart(2, '0');
                    newRow[field] = `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
                }
            });
            return newRow;
        });

        // 创建一个工作簿
        const workbook = XLSX.utils.book_new();
        // 将格式化后的查询结果转换为工作表
        const worksheet = XLSX.utils.json_to_sheet(formattedRows);
        // 将工作表添加到工作簿
        XLSX.utils.book_append_sheet(workbook, worksheet, 'aoyu_data');

        // 将工作簿转换为二进制数据
        const xlsxData = XLSX.write(workbook, { bookType: 'xlsx', type: 'buffer' });

        // 设置响应头，指定文件类型和文件名
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.setHeader('Content-Disposition', 'attachment; filename=aoyu_data.xlsx');

        // 发送文件数据
        res.send(xlsxData);
    } catch (error) {
        console.error(`导出 aoyu_data 数据失败: ${error.message}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    } finally {
        conn.release();
    }
});
app.get('/getsku', async (req, res) => {
    const { startTime, endTime } = req.query;
    const conn = await pool.getConnection();
    try {
        let query = `SELECT 客户名称, SKU, 出库时间 FROM aoyu_data`;
        const queryParams = [];
        if (startTime && endTime) {
            query += ` WHERE 出库时间 BETWEEN ? AND ?`;
            queryParams.push(startTime, endTime);
        } else if (startTime) {
            query += ` WHERE 出库时间 >= ?`;
            queryParams.push(startTime);
        } else if (endTime) {
            query += ` WHERE 出库时间 <= ?`;
            queryParams.push(endTime);
        }

        const [rows] = await conn.query(query, queryParams);

        const dateCustomerSkuCount = {};
        rows.forEach(row => {
            const customerName = row.客户名称;
            const sku = row.SKU;
            const createTime = row.出库时间;
            const date = new Date(createTime).toISOString().split('T')[0]; // 提取日期部分
            
            if (sku) {
                const skuItems = sku.split('\n');
                skuItems.forEach(item => {
                    const parts = item.split('*');
                    if (parts.length === 2) {
                        const skuName = parts[0];
                        const quantity = parseInt(parts[1], 10);
                        if (!isNaN(quantity)) {
                            // 初始化日期层级
                            if (!dateCustomerSkuCount[date]) {
                                dateCustomerSkuCount[date] = {};
                            }
                            
                            // 初始化客户层级
                            if (!dateCustomerSkuCount[date][customerName]) {
                                dateCustomerSkuCount[date][customerName] = {};
                            }
                            
                            // 初始化SKU层级并累加数量
                            if (!dateCustomerSkuCount[date][customerName][skuName]) {
                                dateCustomerSkuCount[date][customerName][skuName] = 0;
                            }
                            dateCustomerSkuCount[date][customerName][skuName] += quantity;
                        }
                    }
                });
            }
        });

        // 转换为扁平化的结果数组
        const result = [];
        for (const date in dateCustomerSkuCount) {
            for (const customerName in dateCustomerSkuCount[date]) {
                for (const skuName in dateCustomerSkuCount[date][customerName]) {
                    result.push({
                        日期: date,
                        客户名称: customerName,
                        SKU名称: skuName,
                        数量: dateCustomerSkuCount[date][customerName][skuName]
                    });
                }
            }
        }

        // 读取并解析库存数据
        const fs = require('fs').promises;
        const skuKcData = JSON.parse(await fs.readFile('./sku_kc.json', 'utf8'));
        
        // 构建库存映射表：{ 客户名称: { sku名称: 库存详情 } }
        const skuKcMap = {};
        skuKcData.forEach(customer => {
            skuKcMap[customer.客户名称] = customer.库存数据.reduce((acc, sku) => {
                acc[sku.sku名称] = sku;
                return acc;
            }, {});
        });

        // 合并库存数据到结果中
        const resultWithStock = result.map(item => {
            const customerStock = skuKcMap[item.客户名称] || {};
            const skuStock = customerStock[item.SKU名称] || { 总库存: 0, 可用库存: 0, 锁定库存: 0, 在途库存: 0 };
            return {
                ...item,
                总库存: skuStock.总库存,
                可用库存: skuStock.可用库存,
                锁定库存: skuStock.锁定库存,
                在途库存: skuStock.在途库存
            };
        });

        res.json({
            success: true,
            data: resultWithStock
        });
    } catch (error) {
        logger.error(`统计每个客户的订单 SKU 数量失败: ${error.message}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    } finally {
        conn.release();
    }
});
app.get('/getsku1', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        // 读取并解析库存数据
        const fs = require('fs').promises;
        const skuKcData = JSON.parse(await fs.readFile('./sku_kc.json', 'utf8'));
        
        // 直接返回库存数据
        res.json({
            success: true,
            data: skuKcData
        });
    } catch (error) {
        logger.error(`获取库存数据失败: ${error.message}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    } finally {
        conn.release();
    }
});

// 每个渠道每日的上网率、妥投率接口
app.get('/gettuotou', async (req, res) => {
    const { startTime, endTime } = req.query;
    const conn = await pool.getConnection();
    try {
        // 设置数据库时区为美国东部时间
        await conn.query("SET time_zone = '-05:00'"); 

        let query = `
            SELECT 
                物流渠道,
                DATE_FORMAT(出库时间, '%Y-%m-%d') AS statistic_date,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN 上网时效 <= 1 THEN 1 ELSE 0 END) AS within_24h_online_count,
                SUM(CASE WHEN 上网时效 <= 2 THEN 1 ELSE 0 END) AS within_48h_online_count,
                SUM(CASE WHEN 妥投时效 <= 3 THEN 1 ELSE 0 END) AS within_3days_delivery_count,
                SUM(CASE WHEN 妥投时效 <= 5 THEN 1 ELSE 0 END) AS within_5days_delivery_count,
                SUM(CASE WHEN 妥投时效 <= 7 THEN 1 ELSE 0 END) AS within_7days_delivery_count,
                SUM(CASE WHEN 妥投时效 > 7 THEN 1 ELSE 0 END) AS over_7days_delivery_count
            FROM 
                aoyu_data
        `;
        const queryParams = [];
        if (startTime && endTime) {
            query += ` WHERE 出库时间 BETWEEN ? AND ?`;
            queryParams.push(startTime, endTime);
        } else if (startTime) {
            query += ` WHERE 出库时间 >= ?`;
            queryParams.push(startTime);
        } else if (endTime) {
            query += ` WHERE 出库时间 <= ?`;
            queryParams.push(endTime);
        }
        // 原有的物流渠道和客户名称筛选条件
        let additionalConditions = [];
        // additionalConditions.push(`物流渠道 NOT LIKE '%上传物流面单%'`);
        additionalConditions.push(`客户名称 NOT LIKE 'DK%' AND 客户名称 NOT IN ('TEST', 'testA')`);
        if (additionalConditions.length > 0) {
            if (queryParams.length > 0) {
                query += ` AND ${additionalConditions.join(' AND ')}`;
            } else {
                query += ` WHERE ${additionalConditions.join(' AND ')}`;
            }
        }

        query += ` GROUP BY 物流渠道, DATE_FORMAT(出库时间, '%Y-%m-%d')`;
        query += ` ORDER BY 物流渠道, statistic_date`;

        const [rows] = await conn.query(query, queryParams);

        const result = rows.map(row => {
            const totalOrders = row.total_orders;
            return {
                channel: row.物流渠道,
                statistic_date: row.statistic_date,
                outbound_time_rates: {
                    '24小时上网率': totalOrders > 0? (row.within_24h_online_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '48小时上网率': totalOrders > 0? (row.within_48h_online_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '3天妥投率': totalOrders > 0? (row.within_3days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '5天妥投率': totalOrders > 0? (row.within_5days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '7天妥投率': totalOrders > 0? (row.within_7days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%',
                    '异常妥投率': totalOrders > 0? (row.over_7days_delivery_count / totalOrders * 100).toFixed(2) + '%' : '0%'
                }
            };
        });

        res.json({
            success: true,
            data: result
        });
    } catch (error) {
        logger.error(`查询每个渠道每日的上网率、妥投率失败: ${error.message}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    } finally {
        conn.release();
    }
});
        
// 登录接口
app.post('/login', async (req, res) => {
    const { username, password } = req.body;
    if (!username || !password) {
        logger.info(`登录尝试失败 - 用户名或密码为空`);
        return res.status(400).json({ 
            success: false, 
            error: '用户名和密码不能为空' 
        });
    }

     const conn = await pool.getConnection();
    try {
        // 先查询用户信息（只获取必要字段）
        const [rows] = await conn.query(
            'SELECT id, username, password_hash, salt FROM users WHERE username = ?', 
            [username]
        );
        
        if (rows.length === 0) {
            logger.info(`登录尝试失败 - 用户名不存在: ${username}`);
            return res.status(401).json({ 
                success: false, 
                error: '用户名或密码错误' 
            });
        }

        const user = rows[0];
        // 验证密码哈希
        const inputHash = crypto
            .createHash('sha256')
            .update(password + user.salt)
            .digest('hex');
            
        if (inputHash !== user.password_hash) {
            logger.info(`登录尝试失败 - 密码错误: ${username}`);
            return res.status(401).json({ 
                success: false, 
                error: '用户名或密码错误' 
            });
        }

        // 生成更安全的token（实际项目应使用JWT）
        const token = crypto
            .createHash('sha256')
            .update(`${username}${Date.now()}${config.server.secretKey}`)
            .digest('hex');

        // 记录登录成功但不要记录敏感信息
        logger.info(`用户 ${username} 登录成功`);
        
        res.json({ 
            success: true, 
            message: '登录成功',
            token,
            hash: user.password_hash, // 返回数据库中的password_hash字段的值
            user: { id: user.id, username: user.username }
        });
    } catch (error) {
        logger.error(`登录查询出错: ${error.stack}`);
        res.status(500).json({ 
            success: false, 
            error: '服务器内部错误' 
        });
    } finally {
        conn.release();
    }
});    
// 修改密码接口
app.post('/changePassword', async (req, res) => {
    const { username, oldPassword, newPassword } = req.body;
    
    if (!username || !oldPassword || !newPassword) {
        return res.status(400).json({ 
            success: false,
            error: '用户名、旧密码和新密码不能为空'
        });
    }

    const conn = await pool.getConnection();
    try {
        // 先验证旧密码
        const [userRows] = await conn.query(
            'SELECT id, password_hash, salt FROM users WHERE username = ?',
            [username]
        );
        
        if (userRows.length === 0) {
            return res.status(401).json({
                success: false,
                error: '用户不存在'
            });
        }

        const user = userRows[0];
        const oldHash = crypto
            .createHash('sha256')
            .update(oldPassword + user.salt)
            .digest('hex');
            
        if (oldHash !== user.password_hash) {
            return res.status(401).json({
                success: false,
                error: '旧密码不正确'
            });
        }

        // 生成新salt和哈希
        const newSalt = crypto.randomBytes(16).toString('hex');
        const newHash = crypto
            .createHash('sha256')
            .update(newPassword + newSalt)
            .digest('hex');

        // 更新数据库
        await conn.query(
            'UPDATE users SET password_hash = ?, salt = ? WHERE id = ?',
            [newHash, newSalt, user.id]
        );
        
        logger.info(`用户 ${username} 密码已更新`);
        res.json({
            success: true,
            message: '密码修改成功'
        });
    } catch (error) {
        logger.error(`修改密码出错: ${error.stack}`);
        res.status(500).json({
            success: false,
            error: '服务器内部错误'
        });
    } finally {
        conn.release();
    }
});
// 将 lastRandomValues 移到接口函数外部
let lastRandomValues = {};

app.get('/getALLResultTest', async (req, res) => {
    try {
        const beijingTime = getBeijingTime();
        const shouldUpdate = req.query.updateTestRates === 'true';
        let data;

        // 尝试从缓存获取上一次生成的随机数据（使用固定缓存键）
        const cachedData = myCache.get('lastTestRatesData');

        if (shouldUpdate) {
            // 生成新随机数据并更新缓存
            data = [];
            for (let i = 0; i < 15; i++) {
                const creationDate = new Date(beijingTime);
                creationDate.setDate(creationDate.getDate() - i);
                const creationDateStr = formatDate(creationDate);
                
                // 判断是否为周日（0）或周一（1）
                const isWeekendOrMonday = creationDate.getDay() === 0 || creationDate.getDay() === 1;
                
                // 修改：outboundDate 与 creationDate 使用相同日期偏移量（i天前）
                const outboundDate = new Date(creationDate); // 直接基于creationDate创建，确保日期一致
                const outboundDateStr = formatDate(outboundDate);
                
                // 每天独立生成随机值（保持原有随机逻辑，或设为0）
                const outbound24h = isWeekendOrMonday ? 0 : Math.min(100, Math.floor(Math.random() * 5) + 97);
                const outbound48h = isWeekendOrMonday ? 0 : Math.min(100, Math.max(outbound24h, Math.floor(Math.random() * 5) + 100));
                
                const online24h = isWeekendOrMonday ? 0 : Math.min(100, Math.floor(Math.random() * 10) + 90);
                const online48h = isWeekendOrMonday ? 0 : Math.min(100, Math.max(online24h, Math.floor(Math.random() * 5) + 95));
                
                const delivery3d = isWeekendOrMonday ? 0 : Math.min(100, Math.floor(Math.random() * 20) + 70);
                const delivery5d = isWeekendOrMonday ? 0 : Math.min(100, Math.max(delivery3d + 1, Math.floor(Math.random() * 15) + 80));
                const delivery7d = isWeekendOrMonday ? 0 : Math.min(100, Math.max(delivery5d + 1, Math.floor(Math.random() * 10) + 90));
                const abnormalDelivery = isWeekendOrMonday ? 0 : Math.max(0, 100 - delivery7d);
                
                data.push({
                    creation_date: creationDateStr,
                    creation_time_rates: {
                        "24小时出库率": `${outbound24h.toFixed(2)}%`,  // 保留两位小数
                        "48小时出库率": `${outbound48h.toFixed(2)}%`   // 保留两位小数
                    },
                    outbound_date: outboundDateStr, // 现在与creation_date日期一致
                    outbound_time_rates: {
                        "24小时上网率": `${online24h.toFixed(2)}%`,    // 保留两位小数
                        "48小时上网率": `${online48h.toFixed(2)}%`,    // 保留两位小数
                        "3天妥投率": `${delivery3d.toFixed(2)}%`,      // 保留两位小数
                        "5天妥投率": `${delivery5d.toFixed(2)}%`,      // 保留两位小数
                        "7天妥投率": `${delivery7d.toFixed(2)}%`,      // 保留两位小数
                        "异常妥投率": `${abnormalDelivery.toFixed(2)}%` // 保留两位小数
                    }
                });
            }
            // 将新生成的数据存入缓存（有效期保持默认1天）
            myCache.set('lastTestRatesData', data);
        } else {
            // 无updateTestRates=true参数时，使用缓存数据
            if (!cachedData) {
                return res.status(400).json({ 
                    success: false, 
                    error: "请先调用一次带updateTestRates=true参数的请求生成数据" 
                });
            }
            data = cachedData;
        }

        res.json({
            success: true,
            data: data
        });
    } catch (error) {
        logger.error(`getALLResultTest 接口出错: ${error.stack}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});
// 新增的 geterror_dingdan 接口
app.get('/geterror_dingdan', async (req, res) => {
    try {
        // 默认 customerCode 为空，可通过查询参数传入
        const customerCode = req.query.customerCode || '';
        // 修改请求数据为表单数据格式
        const formData = new URLSearchParams();
        formData.append('customerCode', customerCode);

        const response = await instance.post('https://omp.xlwms.com/gateway/omp/customer/getTokenForOms', formData, {
            // 直接使用 config 中的 headers
            headers: {
                ...config.externalApi.headers,
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            timeout: 15000
        });

        res.json({
            success: true,
            data: response.data
        });
    } catch (error) {
        if (error.response) {
            // 服务器响应状态码非 2xx
            logger.error(`请求失败，状态码: ${error.response.status}，响应数据: ${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            // 有请求但无响应
            logger.error('请求发送成功，但没有收到响应');
        } else {
            // 设置请求时出错
            logger.error(`请求设置出错: ${error.message}`);
        }
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});
// 新增的 geterror_dingdan_caogao 接口
app.get('/geterror_dingdancaogao', async (req, res) => {
    try {
        const { outboundOrderNos, authorizationToken } = req.query;
        if (!outboundOrderNos) {
            return res.status(400).json({
                success: false,
                error: '缺少 outboundOrderNos 参数'
            });
        }

        if (!authorizationToken) {
            return res.status(400).json({
                success: false,
                error: '缺少 authorizationToken 参数'
            });
        }

        const outboundOrderNosArray = outboundOrderNos.split(',');

        const response = await instance.post('https://oms.xlwms.com/gateway/woms/outboundOrder/draft/batch', {
            outboundOrderNos: outboundOrderNosArray
        }, {
            headers: {
                "Accept": "application/json, text/plain, */*",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh-CN,zh;q=0.9",
                // 使用传入的 authorizationToken
                "Authorization": `Bearer ${authorizationToken}`,
                "Connection": "keep-alive",
                "Content-Type": "application/json;charset=UTF-8",
                "Cookie": "language=cn; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22195604e5ae1a48-00b54a1894e4f5d-26021e51-2073600-195604e5ae2d00%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTk1NjA0ZTVhZTFhNDgtMDBiNTRhMTg5NGU0ZjVkLTI2MDIxZTUxLTIwNzM2MDAtMTk1NjA0ZTVhZTJkMDAifQ%3D%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%7D; version=prod; _hjSessionUser_3119560=eyJpZCI6Ijk4MGU4MDFmLTVkYzUtNTRlNy04NmU4LTQ5OTE1ODk5ZDZkMCIsImNyZWF0ZWQiOjE3NDEwNzc1MDMxMTYsImV4aXN0aW5nIjp0cnVlfQ==; _gid=GA1.2.1847758762.1741572024; _hjSession_3119560=eyJpZCI6IjMwMjQ1MzQ0LTQ5YjctNDcwMy1hOWYyLWExOGFhMWRmMjVhOSIsImMiOjE3NDE3NDU5NTIxMDIsInMiOjAsInIiOjAsInNiIjowLCJzciI6MX0=; _ga_NRLS16EKKE=GS1.1.1741748917.38.0.1741748917.0.0.0; _ga=GA1.1.1880213018.1741077503; _ga_2HTV43T3DN=GS1.1.1741745951.33.1.1741748921.0.0.0; sidebarStatus=0; prod=always",
                "Host": "omp.xlwms.com",
                "Origin": "https://omp.xlwms.com",
                "Referer": "https://omp.xlwms.com/globalOrder/parcel",
                "sec-ch-ua": "\"Google Chrome\";v=\"107\", \"Chromium\";v=\"107\", \"Not=A?Brand\";v=\"24\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
                "version": "prod"
            },
            timeout: 15000
        });

        res.json({
            success: true,
            data: response.data
        });
    } catch (error) {
        if (error.response) {
            // 服务器响应状态码非 2xx
            logger.error(`请求失败，状态码: ${error.response.status}，响应数据: ${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            // 有请求但无响应
            logger.error('请求发送成功，但没有收到响应');
        } else {
            // 设置请求时出错
            logger.error(`请求设置出错: ${error.message}`);
        }
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});
// 新增的 geterror_dingdan_caogao 接口
app.get('/geterror_dingdanct', async (req, res) => {
    try {
        const { outboundOrderNos, authorizationToken } = req.query;
        if (!outboundOrderNos) {
            return res.status(400).json({
                success: false,
                error: '缺少 outboundOrderNos 参数'
            });
        }

        if (!authorizationToken) {
            return res.status(400).json({
                success: false,
                error: '缺少 authorizationToken 参数'
            });
        }

        const outboundOrderNosArray = outboundOrderNos.split(',');

        const response = await instance.post('https://oms.xlwms.com/gateway/woms/outboundOrder/forecast/batch', {
            outboundOrderNos: outboundOrderNosArray
        }, {
            headers: {
                "Accept": "application/json, text/plain, */*",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh-CN,zh;q=0.9",
                // 使用传入的 authorizationToken
                "Authorization": `Bearer ${authorizationToken}`,
                "Connection": "keep-alive",
                "Content-Type": "application/json;charset=UTF-8",
                "Cookie": "language=cn; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22195604e5ae1a48-00b54a1894e4f5d-26021e51-2073600-195604e5ae2d00%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTk1NjA0ZTVhZTFhNDgtMDBiNTRhMTg5NGU0ZjVkLTI2MDIxZTUxLTIwNzM2MDAtMTk1NjA0ZTVhZTJkMDAifQ%3D%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%7D; version=prod; _hjSessionUser_3119560=eyJpZCI6Ijk4MGU4MDFmLTVkYzUtNTRlNy04NmU4LTQ5OTE1ODk5ZDZkMCIsImNyZWF0ZWQiOjE3NDEwNzc1MDMxMTYsImV4aXN0aW5nIjp0cnVlfQ==; _gid=GA1.2.1847758762.1741572024; _hjSession_3119560=eyJpZCI6IjMwMjQ1MzQ0LTQ5YjctNDcwMy1hOWYyLWExOGFhMWRmMjVhOSIsImMiOjE3NDE3NDU5NTIxMDIsInMiOjAsInIiOjAsInNiIjowLCJzciI6MX0=; _ga_NRLS16EKKE=GS1.1.1741748917.38.0.1741748917.0.0.0; _ga=GA1.1.1880213018.1741077503; _ga_2HTV43T3DN=GS1.1.1741745951.33.1.1741748921.0.0.0; sidebarStatus=0; prod=always",
                "Host": "omp.xlwms.com",
                "Origin": "https://omp.xlwms.com",
                "Referer": "https://omp.xlwms.com/globalOrder/parcel",
                "sec-ch-ua": "\"Google Chrome\";v=\"107\", \"Chromium\";v=\"107\", \"Not=A?Brand\";v=\"24\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
                "version": "prod"
            },
            timeout: 15000
        });

        res.json({
            success: true,
            data: response.data
        });
    } catch (error) {
        if (error.response) {
            // 服务器响应状态码非 2xx
            logger.error(`请求失败，状态码: ${error.response.status}，响应数据: ${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            // 有请求但无响应
            logger.error('请求发送成功，但没有收到响应');
        } else {
            // 设置请求时出错
            logger.error(`请求设置出错: ${error.message}`);
        }
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});

// 新增的 geterror_dingdanct1 接口
app.post('/geterror_dingdanct1', async (req, res) => {
    try {
        // 从请求体中获取 orderLists 数据
        const { orderLists } = req.body;
        if (!orderLists || !Array.isArray(orderLists)) {
            return res.status(400).json({
                success: false,
                error: '缺少有效的 orderLists 数组参数'
            });
        }

        // 新增：校验每个元素是否包含 outboundOrderNo 和 customerCode
        const invalidItem = orderLists.find(item => 
            !item.outboundOrderNo || !item.customerCode
        );
        if (invalidItem) {
            return res.status(400).json({
                success: false,
                error: `orderLists 中存在无效项：需要包含 outboundOrderNo 和 customerCode 字段（示例项：${JSON.stringify(invalidItem)}）`
            });
        }

        // 发送请求到外部 API（使用与 geterror_dingdanct 一致的请求头）
        const response = await instance.post(
            'https://omp.xlwms.com/gateway/omp/order/oms/labelRetry/batch',
            { orderLists },  // 直接传递客户端发送的 orderLists 数组（包含 outboundOrderNo 和 customerCode）
            {
                headers: config.externalApi.headers,
                timeout: 15000
            }
        );

        res.json({
            success: true,
            data: response.data
        });
    } catch (error) {
        if (error.response) {
            logger.error(`geterror_dingdanct1 请求失败，状态码: ${error.response.status}，响应数据: ${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            logger.error('geterror_dingdanct1 请求发送成功，但没有收到响应');
        } else {
            logger.error(`geterror_dingdanct1 请求设置出错: ${error.message}`);
        }
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});
// 新增的 get_dingdanctq 接口
app.post('/get_dingdanctq', async (req, res) => {
    try {
        // 从请求体中获取前端传递的 orderLists 数组（字段为中文：出库单号、客户代码）
        const { orderLists } = req.body;
        
        // 校验数据格式
        if (!orderLists || !Array.isArray(orderLists)) {
            logger.warn('/get_dingdanctq 接口接收到无效的orderLists参数');
            return res.status(400).json({ 
                success: false, 
                error: '参数错误：需要提供有效的orderLists数组' 
            });
        }

        // 转换字段名：将中文键名转为外部API要求的英文键名
        const formattedOrderLists = orderLists.map(item => ({
            outboundOrderNo: item.出库单号,  // 出库单号 → outboundOrderNo
            customerCode: item.客户代码       // 客户代码 → customerCode
        }));

        // 记录发送到外部API的数据（转换后的格式）
        logger.info(`/get_dingdanctq 接口发送数据：${JSON.stringify(formattedOrderLists)}`);

        // 调用外部API（使用转换后的 formattedOrderLists）
        const response = await instance.post(
            'https://omp.xlwms.com/gateway/omp/order/oms/labelRetry/batch',
            { orderLists: formattedOrderLists },  // 传递转换后的数组
            { headers: config.externalApi.headers }  // 使用配置中的请求头
        );

        res.json({
            success: true,
            data: response.data  // 返回外部API的原始响应数据
        });
    } catch (error) {
        // 错误日志记录（复用现有日志模块）
        logger.error(`/get_dingdanctq 接口异常: ${error.stack}`);
        
        // 区分错误类型返回响应
        if (error.response) {
            res.status(error.response.status).json({
                success: false,
                error: '外部API请求失败',
                details: error.response.data  // 包含外部API的具体错误信息
            });
        } else {
            res.status(500).json({
                success: false,
                error: '服务器内部错误'
            });
        }
    }
});
// 新增：获取近两周客户周总单量接口
app.get('/getweek_dingdan', async (req, res) => {
    try {
        const beijingTime = getBeijingTime(); // 获取北京时间
        const currentDate = new Date(beijingTime);
        // 获取当前日期的前一天
        const yesterday = new Date(currentDate);
        yesterday.setDate(currentDate.getDate() - 1);

        // 计算上上周和上周的日期范围
        const lastTwoWeekStart = new Date(yesterday);
        lastTwoWeekStart.setDate(yesterday.getDate() - 14);
        const lastTwoWeekEnd = new Date(yesterday);
        lastTwoWeekEnd.setDate(yesterday.getDate() - 8);

        const lastWeekStart = new Date(yesterday);
        lastWeekStart.setDate(yesterday.getDate() - 7);
        const lastWeekEnd = yesterday;

        // 新增日期格式化函数，确保传递给 SQL 的日期格式正确
        function formatSQLDate(date, isEnd = false) {
            const year = date.getFullYear();
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
            return `${year}-${month}-${day} ${isEnd ? '23:59:59' : '00:00:00'}`;
        }

        // 参数化SQL查询
        const [rows] = await pool.query(`
            SELECT 
                客户名称 AS 客户名称,
                CASE 
                    WHEN 出库时间 >= ? AND 出库时间 < ? THEN '上上周（${formatDate(lastTwoWeekStart)}至${formatDate(lastTwoWeekEnd)}）'
                    WHEN 出库时间 >= ? AND 出库时间 <= ? THEN '上周（${formatDate(lastWeekStart)}至${formatDate(lastWeekEnd)}）'
                END AS 周区间,
                COUNT(*) AS 总单量
            FROM aoyu_data
            WHERE 出库时间 >= ? AND 出库时间 <= ?
            GROUP BY 客户名称, 周区间
            ORDER BY 客户名称, 周区间
        `, [
            formatSQLDate(lastTwoWeekStart), formatSQLDate(lastTwoWeekEnd, true),  // 上上周条件
            formatSQLDate(lastWeekStart), formatSQLDate(lastWeekEnd, true),        // 上周条件
            formatSQLDate(lastTwoWeekStart), formatSQLDate(lastWeekEnd, true)      // 总时间范围
        ]);

        res.json({
            success: true,
            data: rows,
            meta: {
                queryRange: `近两周（${formatDate(lastTwoWeekStart)}至${formatDate(lastWeekEnd)}）`,
                note: `上上周（${formatDate(lastTwoWeekStart)}至${formatDate(lastTwoWeekEnd)}），上周（${formatDate(lastWeekStart)}至${formatDate(lastWeekEnd)}）`
            }
        });
    } catch (error) {
        logger.error(`/getweek_dingdan 接口异常: ${error.stack}`);
        res.status(500).json({
            success: false,
            error: '服务器内部错误',
            details: error.message
        });
    }
});

// 获取订单数据并发送通知的主函数
async function fetchAndSendOrderAnalysis() {
  try {
    // 1. 获取订单数据
    const orderData = await fetchOrderData();
    if (!orderData || !orderData.data || orderData.data.length === 0) {
      logger.info('未获取到订单数据，跳过通知');
      return;
    }
    
    // 打印原始数据用于调试
    logger.debug('获取到的原始订单数据:', JSON.stringify(orderData.data, null, 2));

    // 2. 分析订单数据
    const analysisResult = analyzeOrderData(orderData.data);
    
    // 打印分析结果用于调试
    logger.debug('订单分析结果:', JSON.stringify(analysisResult, null, 2));

    // 3. 发送钉钉通知
    if (analysisResult.increaseList.length === 0 && analysisResult.decreaseList.length === 0) {
      logger.info('没有检测到订单变化，跳过通知');
      return;
    }
    
    await sendDingTalkNotification(analysisResult);
    logger.info('订单分析通知发送成功');
  } catch (error) {
    logger.error(`订单分析流程出错: ${error.message}`);
  }
}

// 获取订单数据
async function fetchOrderData() {
  try {
    const response = await axios.get('https://wdbso.vip/getweek_dingdan');
    return response.data;
  } catch (error) {
    logger.error(`获取订单数据失败: ${error.message}`);
    throw error;
  }
}

// 分析订单数据，区分增加和减少的情况
function analyzeOrderData(data) {
  // 按客户名称分组数据
  const customerMap = new Map();
  data.forEach(item => {
    const customerName = item['客户名称'];
    const weekRange = item['周区间'];
    const orderCount = item['总单量'];

    if (!customerMap.has(customerName)) {
      customerMap.set(customerName, {});
    }
    customerMap.get(customerName)[weekRange] = orderCount;
  });

  // 计算每个客户的订单变化
  const increaseList = [];
  const decreaseList = [];

  customerMap.forEach((weeks, customerName) => {
    // 寻找上上周和上周的数据
    let lastWeekData = null;
    let currentWeekData = null;
    
    // 更灵活地匹配周区间
    Object.keys(weeks).forEach(weekKey => {
      if (weekKey.includes('上上周')) {
        lastWeekData = weeks[weekKey];
      } else if (weekKey.includes('上周')) {
        currentWeekData = weeks[weekKey];
      }
    });

    if (lastWeekData !== null && currentWeekData !== null) {
      const diff = currentWeekData - lastWeekData;

      if (diff > 0) {
        increaseList.push({
          customerName,
          lastWeekCount: lastWeekData,
          currentWeekCount: currentWeekData,
          increase: diff
        });
      } else if (diff < 0) {
        decreaseList.push({
          customerName,
          lastWeekCount: lastWeekData,
          currentWeekCount: currentWeekData,
          decrease: Math.abs(diff)
        });
      }
    }
  });

  // 按增减量排序
  increaseList.sort((a, b) => b.increase - a.increase);
  decreaseList.sort((a, b) => b.decrease - a.decrease);

  return {
    increaseList,
    decreaseList,
    totalIncrease: increaseList.reduce((sum, item) => sum + item.increase, 0),
    totalDecrease: decreaseList.reduce((sum, item) => sum + item.decrease, 0)
  };
}

// 发送钉钉通知
async function sendDingTalkNotification(result) {
  try {
    // 获取当前日期，用于计算周区间
    const beijingTime = getBeijingTime();
    const currentDate = new Date(beijingTime);
    
    // 计算当前周的周一
    const currentMonday = new Date(currentDate);
    currentMonday.setDate(currentDate.getDate() - (currentDate.getDay() || 7) + 1);
    
    // 计算上上周和上周的日期范围
    const lastTwoWeekStart = new Date(currentMonday);
    lastTwoWeekStart.setDate(currentMonday.getDate() - 14);
    const lastTwoWeekEnd = new Date(currentMonday);
    lastTwoWeekEnd.setDate(currentMonday.getDate() - 8);
    
    const lastWeekStart = new Date(currentMonday);
    lastWeekStart.setDate(currentMonday.getDate() - 7);
    const lastWeekEnd = new Date(currentMonday);
    lastWeekEnd.setDate(currentMonday.getDate() - 1);
    
    // 格式化日期显示
    const formatDateForDisplay = (date) => {
      const month = date.getMonth() + 1;
      const day = date.getDate();
      return `${month}月${day}日`;
    };
    
    const lastTwoWeekRange = `${formatDateForDisplay(lastTwoWeekStart)}至${formatDateForDisplay(lastTwoWeekEnd)}`;
    const lastWeekRange = `${formatDateForDisplay(lastWeekStart)}至${formatDateForDisplay(lastWeekEnd)}`;

    // 构建通知消息，包含周区间信息
    let message = `📊【订单变化通知】（${lastTwoWeekRange} - ${lastWeekRange}）\n\n`;
    
    if (result.increaseList.length > 0) {
      message += `📈 ${lastWeekRange}订单增加客户（Top 5）:\n`;
      result.increaseList.slice(0, 5).forEach(item => {
        message += `  - ${item.customerName}: 增加 ${item.increase} 单（${item.lastWeekCount} → ${item.currentWeekCount}）\n`;
      });
      message += `  总计增加: ${result.totalIncrease} 单\n\n`;
    } else {
      message += `📈 ${lastWeekRange}没有客户订单增加\n\n`;
    }

    if (result.decreaseList.length > 0) {
      message += `📉 ${lastWeekRange}订单减少客户（Top 5）:\n`;
      result.decreaseList.slice(0, 5).forEach(item => {
        message += `  - ${item.customerName}: 减少 ${item.decrease} 单（${item.lastWeekCount} → ${item.currentWeekCount}）\n`;
      });
      message += `  总计减少: ${result.totalDecrease} 单\n\n`;
    } else {
      message += `📉 ${lastWeekRange}没有客户订单减少\n\n`;
    }

    // message += '查看详情: [订单分析平台](https://wdbso.vip)';

    // 发送到钉钉Webhook
    const webhookUrl = 'https://connector.dingtalk.com/webhook/flow/1031b745b4fd0b116281000x';
    await axios.post(webhookUrl, {
      msgtype: 'text',
      text: {
        content: message
      }
    });
  } catch (error) {
    logger.error(`发送钉钉通知失败: ${error.message}`);
    throw error;
  }
}
// 新增 getcustomer_balance 接口
app.get('/getcustomer_balance', async (req, res) => {
    try {
        const baseUrl = 'https://omp.xlwms.com/gateway/omp/customer/list';
        const size = 50;
        let currentPage = 1;
        let allRecords = [];
        let total = 0;

        // 首次请求获取总数据量
        const firstUrl = `${baseUrl}?current=${currentPage}&size=${size}&status=0&type=1`;
        const firstResponse = await instance.get(firstUrl, { headers: config.externalApi.headers });

        if (firstResponse.status!== 200) {
            throw new Error(`首次请求失败，状态码: ${firstResponse.status}`);
        }

        total = firstResponse.data.data.total;
        const pageCount = Math.ceil(total / size);

        allRecords = allRecords.concat(firstResponse.data.data.records);

        // 后续分页请求
        for (let i = 2; i <= pageCount; i++) {
            const url = `${baseUrl}?current=${i}&size=${size}&status=0&type=1`;
            const response = await instance.get(url, { headers: config.externalApi.headers });

            if (response.status!== 200) {
                throw new Error(`第 ${i} 页请求失败，状态码: ${response.status}`);
            }

            allRecords = allRecords.concat(response.data.data.records);
        }

        const result = allRecords
            .filter(record => {
                const customerName = (record.customerName || '').toLowerCase();
                const customerCode = (record.customerCode || '').toLowerCase();
                // 过滤掉客户名称以 DK 开头以及客户名称或代码包含 test 的记录
                return!record.customerName.startsWith('DK') &&!customerName.includes('test') &&!customerCode.includes('test');
            })
            .map(record => {
                const usdBalance = record.holdValues.find(item => item.currencyCode === 'USD')?.amount || '0.0000';
                const usdCredit = record.creditValues.find(item => item.currencyCode === 'USD')?.amount || '0.0000';
                // 计算总余额
                const totalBalance = (parseFloat(usdBalance) + parseFloat(usdCredit)).toFixed(4);
                return {
                    客户: record.customerName,
                    客户代码: record.customerCode,
                    余额: usdBalance,
                    信用额度: usdCredit,
                    总余额: totalBalance
                };
            });

        // 读取 data.json 文件
        const dataFilePath = path.join(__dirname, 'data.json');
        let historicalData = [];
        if (fs.existsSync(dataFilePath)) {
            const data = fs.readFileSync(dataFilePath, 'utf8');
            if (data.trim()) {
                historicalData = JSON.parse(data);
            }
        }

        const resultWithDays = result.map(customer => {
            if (historicalData.length === 0) {
                return {
                    ...customer,
                    剩余可用天数: '无法计算'
                };
            }

            // 使用现有的数据计算
            const availableDaysData = historicalData;

            // 计算每个客户的平均每日消耗
            const customerDailyConsumption = {};
            availableDaysData.forEach((dayData, index) => {
                dayData.data.forEach(customerData => {
                    const customerKey = customerData.客户代码;
                    if (!customerDailyConsumption[customerKey]) {
                        customerDailyConsumption[customerKey] = [];
                    }
                    // 假设前一天总余额减去当天总余额为当日消耗
                    if (index > 0) {
                        const prevDayCustomer = availableDaysData[index - 1].data.find(c => c.客户代码 === customerKey);
                        if (prevDayCustomer) {
                            const prevTotalBalance = parseFloat(prevDayCustomer.总余额);
                            const currentTotalBalance = parseFloat(customerData.总余额);
                            const consumption = prevTotalBalance - currentTotalBalance;
                            if (consumption > 0) {
                                customerDailyConsumption[customerKey].push(consumption);
                            }
                        }
                    }
                });
            });

            // 计算每个客户的平均每日消耗
            const customerAverageConsumption = {};
            Object.keys(customerDailyConsumption).forEach(customerKey => {
                const consumptionList = customerDailyConsumption[customerKey];
                if (consumptionList.length > 0) {
                    const totalConsumption = consumptionList.reduce((sum, val) => sum + val, 0);
                    customerAverageConsumption[customerKey] = totalConsumption / consumptionList.length;
                }
            });

            const customerKey = customer.客户代码;
            const totalBalance = parseFloat(customer.总余额);
            const averageConsumption = customerAverageConsumption[customerKey] || 0;
            const remainingDays = averageConsumption > 0 ? totalBalance / averageConsumption : '0';
            return {
                ...customer,
                剩余可用天数: typeof remainingDays === 'number' ? remainingDays.toFixed(2) : remainingDays
            };
        });

        res.json({
            success: true,
            data: resultWithDays,
            total,
            size
        });
    } catch (error) {
        if (error.response) {
            // 服务器响应状态码非 2xx
            logger.error(`请求失败，状态码: ${error.response.status}，响应数据: ${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            // 有请求但无响应
            logger.error('请求发送成功，但没有收到响应');
        } else {
            // 设置请求时出错
            logger.error(`请求设置出错: ${error.message}`);
        }
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});
// 新增：获取客户余额并推送钉钉的定时任务
async function fetchCustomerBalance() {
    try {
        // 获取客户余额数据
        const balanceResponse = await instance.get('https://wdbso.vip/getcustomer_balance');
        
        // 检查响应格式
        if (!balanceResponse.data || !Array.isArray(balanceResponse.data.data)) {
            logger.error('客户余额接口返回数据格式不正确');
            return [];
        }
        
        const allCustomers = balanceResponse.data.data;
        logger.info(`获取到 ${allCustomers.length} 个客户的余额数据`);

        // 筛选条件：
        // 1. 余额 < 100 或 剩余可用天数 < 3
        // 2. 总余额 ≠ 0
        // 3. 剩余可用天数 ≠ 0
        const filteredCustomers = allCustomers.filter(item => {
            const balance = parseFloat(item.余额) || 0;
            const availableDays = parseFloat(item.剩余可用天数) || 0;
            const totalBalance = parseFloat(item.总余额) || 0;
            
            return (
                (balance < 100 || availableDays < 3) && 
                totalBalance !== 0
            );
        });
        
        logger.info(`筛选后符合条件的客户数：${filteredCustomers.length}`);
        return filteredCustomers;
    } catch (error) {
        logger.error(`获取客户余额失败: ${error.stack}`);
        return [];
    }
}

async function sendDingTalkAlert() {
    try {
        const filteredCustomers = await fetchCustomerBalance();
        // 只保留紧急情况的客户：余额<100且剩余天数<3
        const emergencyCustomers = filteredCustomers.filter(c => {
            const balance = parseFloat(c.余额) || 0;
            const days = parseFloat(c.剩余可用天数) || 0;
            return balance < 100 && days < 3;
        });

        if (emergencyCustomers.length === 0) {
            logger.info('无需要提醒的紧急客户');
            return;
        }

        // 对紧急客户按余额和天数排序（余额低且天数少的排前面）
        emergencyCustomers.sort((a, b) => {
            const aBalance = parseFloat(a.余额) || 0;
            const aDays = parseFloat(a.剩余可用天数) || 0;
            const bBalance = parseFloat(b.余额) || 0;
            const bDays = parseFloat(b.剩余可用天数) || 0;
            
            // 先按剩余天数排序，天数少的优先
            if (aDays !== bDays) {
                return aDays - bDays;
            }
            
            // 天数相同则按余额排序，余额少的优先
            return aBalance - bBalance;
        });

        // 构造紧急客户的markdown文本
        const markdownItems = emergencyCustomers.map((customer, index) => {
            return `${index + 1}. **🔴 紧急 🔴** ${customer.客户}(${customer.客户代码}), 余额${customer.余额}, 信用额度${customer.信用额度}, 总余额${customer.总余额}, 剩余可用天数${customer.剩余可用天数}`;
        }).join('\n  ');

        const dingTalkData = {
            msgtype: 'markdown',
            markdown: {
                title: `紧急！需要立即充值的客户（共${emergencyCustomers.length}个）`,
                text: `# ⚠️ 紧急提醒：需立即处理的客户充值\n  ${markdownItems}\n\n**紧急说明**: 以上客户余额不足100元且剩余可用天数不足3天，可能导致服务中断！\n\n**总计**: ${emergencyCustomers.length}个紧急客户需要立即关注\n[广播] [数据源](https://wdbso.vip)`
            },
            at: { isAtAll: true } // 依然@所有人，因为紧急情况需要立即关注
        };

        // 发送钉钉通知（保持原有重试机制）
        const maxRetries = 3;
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                await instance.post('https://connector.dingtalk.com/webhook/flow/1031b1ce395b21045113000u', dingTalkData);
                logger.info(`成功推送紧急客户钉钉通知，涉及客户数：${emergencyCustomers.length}`);
                break;
            } catch (error) {
                if (attempt === maxRetries) {
                    logger.error(`紧急客户钉钉通知发送失败（尝试${attempt}/${maxRetries}）: ${error.message}`);
                    throw error;
                } else {
                    logger.warn(`紧急客户钉钉通知发送失败（尝试${attempt}/${maxRetries}）: ${error.message}，将在${attempt * 2}秒后重试`);
                    await new Promise(resolve => setTimeout(resolve, attempt * 2000));
                }
            }
        }
    } catch (error) {
        logger.error(`紧急客户钉钉通知处理失败: ${error.stack}`);
    }
}


// 新增 get_meixi 接口
app.get('/get_meixi', async (req, res) => {
    try {
        const beijingTime = getBeijingTime();
        // 获取昨日日期
        const yesterday = new Date(beijingTime);
        yesterday.setDate(yesterday.getDate() - 1);
        const yesterdayStr = formatDate(yesterday);
        const ninePM = new Date(`${yesterdayStr} 21:00:00`);

        const initialRequestData = {
            logisticsChannel: "",
            unitMark: 0,
            whCode: "CA01",
            timeType: "createTime",
            startTime: `${yesterdayStr} 00:00:00`,
            endTime: `${yesterdayStr} 23:59:59`,
            current: 1,
            size: 200,
            total: 0,
            platformCode: 1
        };

        let allRecords = [];
        let currentPage = 1;
        let totalPages = 1;

        do {
            const requestData = {
                ...initialRequestData,
                current: currentPage
            };

            const response = await instance.post('https://omp.xlwms.com/gateway/omp/order/delivery/page', requestData, {
                headers: config.externalApi.headers
            });

            const records = response.data.data.records;
            allRecords = allRecords.concat(records);

            totalPages = response.data.data.pages;
            currentPage++;
        } while (currentPage <= totalPages);

        // 过滤掉已取消的订单
        const nonCanceledOrders = allRecords.filter(record => record.status !== 99);

        // 统计总订单数
        const totalOrders = nonCanceledOrders.length;

        // 统计应出库订单数（不包含已取消的，状态 30 及以上为应出库状态）
        const shouldOutboundOrders = nonCanceledOrders.filter(record => record.status >= 30).length;

        // 统计截单前的订单数（不包含已取消的）
        const ordersBeforeCutoff = nonCanceledOrders.filter(record => {
            const createTime = new Date(record.createTime);
            return createTime < ninePM;
        });
        const ordersBeforeCutoffCount = ordersBeforeCutoff.length;

        // 统计实际出库订单数
        const actuallyOutboundOrders = nonCanceledOrders.filter(record => record.status === 100).length;

        // 统计实际截单前的单出库数
        const actuallyOutboundBeforeCutoff = ordersBeforeCutoff.filter(record => record.status === 100).length;

        const result = {
            总订单数: totalOrders,
            应出库订单数: shouldOutboundOrders,
            实际出库订单数: actuallyOutboundOrders,
            截单前订单数: ordersBeforeCutoffCount,
            实际截单前出库订单数: actuallyOutboundBeforeCutoff
        };

        res.json({
            success: true,
            data: result
        });
    } catch (error) {
        if (error.response) {
            // 服务器响应状态码非 2xx
            logger.error(`请求失败，状态码: ${error.response.status}，响应数据: ${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            // 有请求但无响应
            logger.error('请求发送成功，但没有收到响应');
        } else {
            // 设置请求时出错
            logger.error(`请求设置出错: ${error.message}`);
        }
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});
app.post('/writer_data', async (req, res) => {
    // 新增 仓库 字段
    const { 渠道, 出库率日期, 出库率, 上网率日期, 上网率, 妥投率日期, 妥投率, 上架率日期, 上架率, 上传日期, 仓库 } = req.body;

    // 修正变量名错误，原代码使用 uploadDate ，实际应为 上传日期
    if (!上传日期) {
        return res.status(400).json({
            success: false,
            error: '上传日期不能为空'
        });
    }

    // 处理日期字段，将空字符串转换为 null
    const handleDate = (date) => date === '' ? null : date;
    const safe出库率日期 = handleDate(出库率日期);
    const safe上网率日期 = handleDate(上网率日期);
    const safe妥投率日期 = handleDate(妥投率日期);
    const safe上架率日期 = handleDate(上架率日期);

    // 处理数值字段，将空字符串转换为 null
    const handleNumber = (num) => num === '' ? null : num;
    const safe出库率 = handleNumber(出库率);
    const safe上网率 = handleNumber(上网率);
    const safe妥投率 = handleNumber(妥投率);
    const safe上架率 = handleNumber(上架率);

    // 处理仓库字段，将空字符串转换为 null
    const safe仓库 = 仓库 === '' ? null : 仓库;

    const conn = await pool.getConnection();
    try {
        const [result] = await conn.query(`
            INSERT INTO data_table (渠道, 出库率日期, 出库率, 上网率日期, 上网率, 妥投率日期, 妥投率, 上架率日期, 上架率, 上传日期, 仓库)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `, [渠道, safe出库率日期, safe出库率, safe上网率日期, safe上网率, safe妥投率日期, safe妥投率, safe上架率日期, safe上架率, 上传日期, safe仓库]);

        res.json({
            success: true,
            message: '数据写入成功',
            insertId: result.insertId
        });
    } catch (error) {
        logger.error(`数据写入 data_table 失败: ${error.stack}`);
        res.status(500).json({
            success: false,
            error: '服务器内部错误',
            details: error.message
        });
    } finally {
        conn.release();
    }
});
app.post('/writer_error', async (req, res) => {
    try {
        const { 
            异常标记日期, 
            异常出现日期, 
            异常类型, 
            异常原因, 
            预计解决时间, 
            最后处理人, 
            是否解决, 
            备注,
            仓库,
            升级处理人  
        } = req.body;

        if (!异常标记日期 || !异常出现日期 || !异常类型) {
            return res.status(400).json({
                success: false,
                error: '异常标记日期、异常出现日期和异常类型为必填字段'
            });
        }

        // 确保是否解决字段在没有传入值时使用默认值 0
        const safe是否解决 = typeof 是否解决 === 'undefined' ? 0 : 是否解决;

        const conn = await pool.getConnection();
        try {
            const [result] = await conn.query(`
                INSERT INTO error_table (
                    异常标记日期, 
                    异常出现日期, 
                    异常类型, 
                    异常原因, 
                    预计解决时间, 
                    最后处理人, 
                    是否解决, 
                    备注,
                    仓库,
                    升级处理人  
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  
            `, [
                异常标记日期, 
                异常出现日期, 
                异常类型, 
                异常原因, 
                预计解决时间, 
                最后处理人, 
                safe是否解决, 
                备注,
                仓库,
                升级处理人  
            ]);

            res.json({
                success: true,
                message: '数据写入成功',
                insertId: result.insertId
            });
        } catch (error) {
            logger.error(`writer_error 接口数据写入失败: ${error.stack}`);
            res.status(500).json({
                success: false,
                error: '服务端错误',
                details: error.message
            });
        } finally {
            conn.release();
        }
    } catch (error) {
        logger.error(`writer_error 接口处理出错: ${error.stack}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});

app.get('/getStatsData', async (req, res) => {
    try {
        const conn = await pool.getConnection();
        const [rows] = await conn.query('SELECT * FROM data_table');
        conn.release();
        res.json({
            success: true,
            data: rows
        });
    } catch (error) {
        logger.error(`getStatsData 接口数据查询失败: ${error.stack}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});

app.get('/getErrorData', async (req, res) => {
    try {
        const conn = await pool.getConnection();
        const [rows] = await conn.query('SELECT * FROM error_table');
        conn.release();
        res.json({
            success: true,
            data: rows
        });
    } catch (error) {
        logger.error(`getErrorData 接口数据查询失败: ${error.stack}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});
app.post('/updata_error', async (req, res) => {
    try {
        const { id, 预计解决时间, 最后处理人, 是否解决, 备注, 升级处理人 } = req.body;

        if (!id) {
            return res.status(400).json({
                success: false,
                error: 'id 不能为空'
            });
        }

        const conn = await pool.getConnection();
        try {
            const [result] = await conn.query(`
                UPDATE error_table
                SET 预计解决时间 = ?, 最后处理人 = ?, 是否解决 = ?, 备注 = ?, 升级处理人 = ?
                WHERE id = ?
            `, [预计解决时间, 最后处理人, 是否解决, 备注, 升级处理人, id]);

            if (result.affectedRows === 0) {
                return res.status(404).json({
                    success: false,
                    error: '未找到对应的记录'
                });
            }

            res.json({
                success: true,
                message: '数据更新成功'
            });
        } catch (error) {
            logger.error(`updata_error 接口数据更新失败: ${error.stack}`);
            res.status(500).json({
                success: false,
                error: '服务端错误',
                details: error.message
            });
        } finally {
            conn.release();
        }
    } catch (error) {
        logger.error(`updata_error 接口处理出错: ${error.stack}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});
app.get('/ruku', async (req, res) => {
    try {
        // 固定每页大小（根据外部接口实际规则调整）
        const size = 200;
        // 第一页请求获取总记录数
        const firstResponse = await instance.get(
            `https://omp.xlwms.com/gateway/omp/order/asn/page?status=&whCode=&current=1&size=${size}`,
            { headers: config.externalApi.headers }
        );

        // 提取总记录数并计算总页数
        const total = firstResponse.data.data?.total || 0;
        const page = Math.ceil(total / size);
        console.log(`总记录数：${total}，总页数：${page}`);

        // 获取客户列表并生成映射
        const customerSize = 50;
        const customerFirstResponse = await instance.get(
            `https://omp.xlwms.com/gateway/omp/customer/list?current=1&size=${customerSize}&type=1`,
            { headers: config.externalApi.headers }
        );
        const customerTotal = customerFirstResponse.data.data?.total || 0;
        const customerPage = Math.ceil(customerTotal / customerSize);
        console.log(`客户总记录数：${customerTotal}，客户总页数：${customerPage}`);

        const customerRequests = [];
        for (let current = 1; current <= customerPage; current++) {
            customerRequests.push(
                instance.get(
                    `https://omp.xlwms.com/gateway/omp/customer/list?current=${current}&size=${customerSize}&type=1`,
                    { headers: config.externalApi.headers }
                )
            );
        }

        const customerResponses = await Promise.all(customerRequests);
        const allCustomers = customerResponses.reduce((acc, res) => {
            return acc.concat(res.data.data?.records || []);
        }, []);

        const customerMap = new Map();
        allCustomers.forEach(customer => {
            if (customer.customerName && customer.customerPeopleName) {
                customerMap.set(customer.customerName, customer.customerPeopleName);
            }
        });

        // 生成所有页的入库单请求数组
        const pageRequests = [];
        for (let current = 1; current <= page; current++) {
            pageRequests.push(
                instance.get(
                    `https://omp.xlwms.com/gateway/omp/order/asn/page?status=&whCode=&current=${current}&size=${size}`,
                    { headers: config.externalApi.headers }
                )
            );
        }

        const responses = await Promise.all(pageRequests);
        const allRecords = responses.reduce((acc, res) => {
            return acc.concat(res.data.data?.records || []);
        }, []);

        // 提取并映射数据（新增"客户填写的跟踪单号"字段）
        const mappedData = allRecords.map(item => {
            // 状态转换逻辑
            let currentStatus;
            switch (item.status) {
                case 10: currentStatus = '待入库'; break;
                case 11: currentStatus = '收货中'; break;
                case 15: currentStatus = '已收货'; break;
                case 30: currentStatus = '已上架'; break;
                case 90: currentStatus = '已取消'; break;
                default: currentStatus = '';
            }

            const 所属客服 = customerMap.get(item.customerName) || '';
            const 参考单号 = item.referenceNo || '';
            
            // 提取客户填写的跟踪单号（trackingNo）
            const 客户填写的跟踪单号 = item.trackingNo || ''; 

            return {
                入库单号: item.sourceNo || '',
                仓库: item.whCodeName || '',
                客户: item.customerName || '',
                所属客服: 所属客服,
                预报箱数或托盘数: item.boxCount || 0,
                上架箱数: item.receiptCount || 0,
                创建日期: item.createTime || null,
                上架时间: item.putawayFinishTime || null,
                当前状态: currentStatus,
                参考单号: 参考单号,
                客户填写的跟踪单号: 客户填写的跟踪单号  
            };
        });

        if (mappedData.length > 0) {
            const sourceNos = mappedData.map(item => item.入库单号);
            const [existingRows] = await pool.query(
                'SELECT 入库单号 FROM ruku_data WHERE 入库单号 IN (?)',
                [sourceNos]
            );
            const existingSourceNos = new Set(existingRows.map(row => row.入库单号));

            const updateData = mappedData.filter(item => existingSourceNos.has(item.入库单号));
            const insertData = mappedData.filter(item => !existingSourceNos.has(item.入库单号));

            // 处理更新逻辑（包含客户填写的跟踪单号）
            if (updateData.length > 0) {
                const updateSql = `
                    UPDATE ruku_data 
                    SET 
                        当前状态 = ?, 
                        上架时间 = ?, 
                        上架箱数 = ?, 
                        所属客服 = ?,
                        参考单号 = ?,
                        客户填写的跟踪单号 = ? 
                    WHERE 入库单号 = ?
                `;
                for (const item of updateData) {
                    await pool.query(updateSql, [
                        item.当前状态,
                        item.上架时间,
                        item.上架箱数,
                        item.所属客服,
                        item.参考单号,
                        item.客户填写的跟踪单号,  // 新增字段参数
                        item.入库单号
                    ]);
                }
                console.log(`成功更新 ${updateData.length} 条记录`);
            }

            // 处理插入逻辑（包含客户填写的跟踪单号）
            if (insertData.length > 0) {
                const insertSql = `
                    INSERT INTO ruku_data (
                        创建日期, 入库单号, 客户, 所属客服, 派送方式, 跟踪单号, 当前状态, 是否已提供POD, 
                        预计到仓时间, 实际到仓时间, 上架时间, 预报箱数或托盘数, 实际到货箱数, 上架箱数, 
                        客服备注, 后端备注, 是否异常, 仓库, 参考单号, 客户填写的跟踪单号  -- 新增独立字段
                    ) VALUES ?
                `;
                const values = insertData.map(item => [
                    item.创建日期,
                    item.入库单号,
                    item.客户,
                    item.所属客服,
                    '',  // 原有派送方式字段
                    '',  // 原有跟踪单号字段（保持不变）
                    item.当前状态,
                    0,
                    null,
                    null,
                    item.上架时间,
                    item.预报箱数或托盘数,
                    0,
                    item.上架箱数,
                    '',
                    '',
                    0,
                    item.仓库,
                    item.参考单号,
                    item.客户填写的跟踪单号  // 新增独立字段值
                ]);
                await pool.query(insertSql, [values]);
                console.log(`成功插入 ${insertData.length} 条新记录`);
            }

            if (updateData.length === 0 && insertData.length === 0) {
                console.log('无记录需要处理');
            }
        }

        res.json({
            success: true,
            data: mappedData,
            totalPages: page
        });
    } catch (error) {
        logger.error(`ruku接口请求出错: ${error.message}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});
    

app.get('/select_ruku_data', async (req, res) => {
  const conn = await pool.getConnection(); // 获取连接
  try {
    // 设置数据库时区（与物流接口保持一致）
    await conn.query("SET time_zone = '-05:00'");

    // 提取请求参数（新增referenceNo用于搜索）
    const {
      warehouse,          // 仓库
      customer,           // 客户
      kefu,               // 所属客服
      status,             // 当前状态（逗号分隔的多值）
      isAbnormal,         // 是否异常（0/1）
      hasKefuRemark,      // 是否有客服备注（0/1）
      hasHdRemark,        // 是否有后端备注（0/1）
      rukuNo,             // 入库单号（支持批量搜索）
      trackingNo,         // 跟踪单号（模糊匹配）
      referenceNo,        // 参考单号（新增：用于批量搜索条件）
      timeFilterType,     // 时间筛选类型
      startDate,          // 开始时间
      endDate             // 结束时间
    } = req.query;

    // 构建查询条件和参数（防止SQL注入）
    const conditions = [];
    const values = [];

    // 仓库筛选（精确匹配）
    if (warehouse) {
      conditions.push('仓库 = ?');
      values.push(warehouse);
    }

    // 客户筛选（精确匹配）
    if (customer) {
      conditions.push('客户 = ?');
      values.push(customer);
    }

    // 所属客服筛选（精确匹配）
    if (kefu) {
      conditions.push('所属客服 = ?');
      values.push(kefu);
    }

    // 当前状态筛选（多值匹配）
    if (status) {
      const statusArr = status.split(',').map(s => s.trim()).filter(s => s);
      conditions.push(`当前状态 IN (${statusArr.map(() => '?').join(',')})`);
      values.push(...statusArr);
    }

    // 是否异常筛选（0/1）
    if (isAbnormal !== undefined && isAbnormal !== '') {
      conditions.push('是否异常 = ?');
      values.push(isAbnormal);
    }

    // 客服备注筛选（是否有值）
    if (hasKefuRemark !== undefined && hasKefuRemark !== '') {
      if (hasKefuRemark === '1') {
        conditions.push('(客服备注 IS NOT NULL AND 客服备注 != "")');
      } else {
        conditions.push('(客服备注 IS NULL OR 客服备注 = "")');
      }
    }

    // 后端备注筛选（是否有值）
    if (hasHdRemark !== undefined && hasHdRemark !== '') {
      if (hasHdRemark === '1') {
        conditions.push('(后端备注 IS NOT NULL AND 后端备注 != "")');
      } else {
        conditions.push('(后端备注 IS NULL OR 后端备注 = "")');
      }
    }

    // 入库单号筛选（支持批量搜索）
    if (rukuNo) {
      let processedRukuNo = rukuNo
        .replace(/[\r\n\s]+/g, ',')
        .replace(/,+/g, ',')
        .replace(/^,|,$/g, '');
      
      const rukuNoArr = processedRukuNo.split(',').filter(no => no.trim() !== '');
      
      if (rukuNoArr.length > 1) {
        conditions.push(`入库单号 IN (${rukuNoArr.map(() => '?').join(',')})`);
        values.push(...rukuNoArr);
      } else if (rukuNoArr.length === 1) {
        conditions.push('入库单号 LIKE ?');
        values.push(`%${rukuNoArr[0]}%`);
      }
    }

    // 跟踪单号筛选（模糊匹配）
    if (trackingNo) {
      conditions.push('跟踪单号 LIKE ?');
      values.push(`%${trackingNo}%`);
    }

    // 参考单号筛选（支持批量搜索，但不返回该字段）
    if (referenceNo) {
      let processedReferenceNo = referenceNo
        .replace(/[\r\n\s]+/g, ',')
        .replace(/,+/g, ',')
        .replace(/^,|,$/g, '');
      
      const referenceNoArr = processedReferenceNo.split(',').filter(no => no.trim() !== '');
      
      if (referenceNoArr.length > 1) {
        conditions.push(`参考单号 IN (${referenceNoArr.map(() => '?').join(',')})`);
        values.push(...referenceNoArr);
      } else if (referenceNoArr.length === 1) {
        conditions.push('参考单号 LIKE ?');
        values.push(`%${referenceNoArr[0]}%`);
      }
    }

    // 时间范围筛选
    if (timeFilterType && (startDate || endDate)) {
      const timeFieldMap = {
        createDate: '创建日期',
        expectedArrival: '预计到仓时间',
        actualArrival: '实际到仓时间',
        shelvingTime: '上架时间'
      };
      const timeField = timeFieldMap[timeFilterType] || '创建日期';

      if (startDate && endDate) {
        conditions.push(`${timeField} BETWEEN ? AND ?`);
        values.push(`${startDate} 00:00:00`, `${endDate} 23:59:59`);
      } else if (startDate) {
        conditions.push(`${timeField} >= ?`);
        values.push(`${startDate} 00:00:00`);
      } else if (endDate) {
        conditions.push(`${timeField} <= ?`);
        values.push(`${endDate} 23:59:59`);
      }
    }

    // 构建WHERE子句
    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    // 执行查询（SELECT中不包含参考单号字段）
    const query = `
      SELECT 
        仓库, 入库单号, 客户, 所属客服, 预报箱数或托盘数, 当前状态,
        创建日期, 上架时间, 派送方式, 跟踪单号, 是否已提供POD,
        预计到仓时间, 实际到仓时间, 上架箱数, 客服备注, 后端备注, 是否异常
      FROM ruku_data
      ${whereClause}
      ORDER BY 创建日期 DESC
    `;

    const [rows] = await conn.query(query, values);

    // 返回结果（结果中不含参考单号）
    res.json({
      success: true,
      data: rows,
      message: `共查询到 ${rows.length} 条数据`,
      total: rows.length
    });

  } catch (error) {
    console.error(`入库单数据查询失败: ${error.message}`);
    res.status(500).json({
      success: false,
      error: '服务端错误',
      details: error.message
    });
  } finally {
    conn.release(); // 释放连接
  }
});


// 确保 ruku.json 文件存在（同步版本）
function ensureRukuFileExists() {
  const filePath = path.join(__dirname, 'ruku.json');
  try {
    fs.accessSync(filePath);
  } catch (error) {
    // 文件不存在，创建并初始化
    fs.writeFileSync(filePath, '[]', 'utf8');
  }
}

// 带重试机制的同步写入日志到 ruku.json
function writeToRukuLog(data) {
  const filePath = path.join(__dirname, 'ruku.json');
  const maxRetries = 3; // 最大重试次数
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`[writeToRukuLog] 开始写入日志 (尝试 ${attempt}/${maxRetries}):`, data);
      
      // 确保文件存在
      ensureRukuFileExists();
      
      // 读取现有内容（同步）
      let content = fs.readFileSync(filePath, 'utf8');
      
      // 处理空文件情况
      let logs = [];
      if (content.trim() !== '') {
        logs = JSON.parse(content);
      }
      
      console.log('[writeToRukuLog] 读取现有日志，共', logs.length, '条记录');
      
      // 添加新日志
      logs.push(data);
      
      // 写回文件（同步）
      fs.writeFileSync(filePath, JSON.stringify(logs, null, 2), 'utf8');
      
      console.log('[writeToRukuLog] 日志写入成功');
      return; // 成功后退出循环
    } catch (error) {
      console.error(`[writeToRukuLog] 写入日志文件失败 (尝试 ${attempt}/${maxRetries}):`, error);
      
      // 仅在不是最后一次尝试时等待并重试
      if (attempt < maxRetries) {
        console.log(`[writeToRukuLog] 等待 100ms 后重试...`);
        // 使用立即执行函数模拟异步延迟
        (function(attempt) {
          return new Promise(resolve => setTimeout(resolve, 100));
        })(attempt);
      } else {
        // 开发环境下抛出错误，便于调试
        if (process.env.NODE_ENV !== 'production') {
          throw new Error(`写入日志文件失败: ${error.message}`);
        }
      }
    }
  }
}

// 新增：客服修改接口（kf_updata）
app.post('/kf_updata', async (req, res) => {
  try {
    const { 入库单号, 派送方式, 跟踪单号, 是否已提供POD, 预计到仓时间, 客服备注, 用户名 } = req.body;
    const submitTime = new Date().toISOString();
    
    // 验证必要参数
    if (!入库单号) {
      return res.status(400).json({ success: false, error: '缺少必要参数：入库单号' });
    }

    // 构造更新SQL（仅修改指定字段）
    const updateSql = `
      UPDATE ruku_data 
      SET 
        派送方式 = ?, 
        跟踪单号 = ?,
        是否已提供POD = ?, 
        预计到仓时间 = ?, 
        客服备注 = ?
      WHERE 入库单号 = ?
    `;

    // 执行更新（参数化查询防注入）
    await pool.query(updateSql, [
      派送方式 || '',        // 派送方式空值处理
      跟踪单号 || '',        // 跟踪单号空值处理
      是否已提供POD || 0,    // 是否已提供POD默认0（否）
      预计到仓时间 || null,   // 预计到仓时间空值设为null
      客服备注 || '',         // 客服备注空值处理
      入库单号                // WHERE条件参数
    ]);

    // 同步记录日志（确保数据库操作成功后执行）
    writeToRukuLog({
      username: 用户名 || '未知用户',
      submitTime,
      action: 'kf_updata',
      入库单号,
      修改内容: {
        派送方式, 跟踪单号, 是否已提供POD, 预计到仓时间, 客服备注
      }
    });

    res.json({ success: true, message: `入库单号 ${入库单号} 客服信息更新成功` });
  } catch (error) {
    logger.error(`kf_updata接口出错: ${error.message}`);
    res.status(500).json({ success: false, error: '服务端错误', details: error.message });
  }
});

// 新增：后端修改接口（hd_updata）
app.post('/hd_updata', async (req, res) => {
  try {
    const { 入库单号, 实际到仓时间, 后端备注, 实际到货箱数, 是否异常, 用户名 } = req.body;
    const submitTime = new Date().toISOString();
    
    // 验证必要参数
    if (!入库单号) {
      return res.status(400).json({ success: false, error: '缺少必要参数：入库单号' });
    }

    // 构造更新SQL（新增是否异常字段）
    const updateSql = `
      UPDATE ruku_data 
      SET 
        实际到仓时间 = ?, 
        后端备注 = ?,
        实际到货箱数 = ?,
        是否异常 = ? 
      WHERE 入库单号 = ?
    `;

    // 执行更新（参数化查询防注入）
    await pool.query(updateSql, [
      实际到仓时间 || null,   // 空值设为null
      后端备注 || '',         // 空值处理
      实际到货箱数 || 0,      // 实际到货箱数默认0
      是否异常 || 0,          // 是否异常默认0（否）
      入库单号                // WHERE条件参数
    ]);

    // 同步记录日志（确保数据库操作成功后执行）
    writeToRukuLog({
      username: 用户名 || '未知用户',
      submitTime,
      action: 'hd_updata',
      入库单号,
      修改内容: {
        实际到仓时间, 后端备注, 实际到货箱数, 是否异常
      }
    });

    res.json({ success: true, message: `入库单号 ${入库单号} 后端信息更新成功` });
  } catch (error) {
    logger.error(`hd_updata接口出错: ${error.message}`);
    res.status(500).json({ success: false, error: '服务端错误', details: error.message });
  }
});
// 修改后的接口：获取指定入库单的日志
app.get('/get_ruku_logs', async (req, res) => {
  try {
    const rukuId = req.query.ruku_id;
    if (!rukuId) {
      return res.status(400).json({ success: false, error: '缺少必要参数', details: '需要提供ruku_id参数' });
    }
    
    const filePath = path.join(__dirname, 'ruku.json');
    
    // 确保文件存在
    ensureRukuFileExists();
    
    // 读取日志文件
    const content = fs.readFileSync(filePath, 'utf8');
    
    // 处理空文件情况
    let allLogs = [];
    if (content.trim() !== '') {
      allLogs = JSON.parse(content);
    }
    
    // 筛选指定入库单的日志
    const filteredLogs = allLogs.filter(log => log.入库单号 === rukuId);
    
    res.json({ success: true, logs: filteredLogs });
  } catch (error) {
    logger.error(`get_ruku_logs接口出错: ${error.message}`);
    res.status(500).json({ success: false, error: '服务端错误', details: error.message });
  }
});
// 新增的 sku_kc 接口
app.get('/sku_kc', async (req, res) => {
    try {
        // 调用外部接口获取完整客户列表（包含名称） - 支持分页
        const customerList = [];
        let currentPage = 1;
        let totalPages = 1;
        
        while (currentPage <= totalPages) {
            try {
                const customerListResponse = await instance.get(`https://omp.xlwms.com/gateway/omp/customer/list?current=${currentPage}&size=50&type=1&customerQuery=`, {
                    headers: config.externalApi.headers
                });
                
                // 校验客户列表响应数据结构
                const pageData = customerListResponse.data?.data;
                if (!pageData || !Array.isArray(pageData.records)) {
                    throw new Error(`获取客户列表第 ${currentPage} 页失败，数据格式异常`);
                }
                
                // 添加当前页的客户记录
                customerList.push(...pageData.records);
                
                // 更新总页数信息
                totalPages = pageData.pages || 1;
                
                logger.info(`已获取客户列表第 ${currentPage}/${totalPages} 页，当前页记录数: ${pageData.records.length}`);
                
                // 准备获取下一页
                currentPage++;
                
                // 为避免频繁请求，添加小延迟
                if (currentPage <= totalPages) {
                    await new Promise(resolve => setTimeout(resolve, 300));
                }
            } catch (error) {
                logger.error(`获取客户列表第 ${currentPage} 页失败: ${error.message}`);
                // 出错时仍然增加页码，避免无限循环
                currentPage++;
            }
        }
        
        if (customerList.length === 0) {
            logger.error('未获取到有效客户列表数据');
            return res.status(404).json({ 
                success: false,
                error: '未获取到客户列表数据' 
            });
        }

        // 创建客户代码到客户名称的映射
        const customerCodeToNameMap = new Map();
        customerList.forEach(customer => {
            const customerCode = parseInt(customer.customerCode, 10);
            if (!isNaN(customerCode) && customer.customerName) {
                customerCodeToNameMap.set(customerCode, customer.customerName);
            }
        });

        // 调用外部接口获取原始客户列表（用于确定代码范围）
        const response = await instance.get('https://omp.xlwms.com/gateway/omp/customer/list?current=1&size=10&type=1&customerQuery=', {
            headers: config.externalApi.headers
        });

        // 校验响应数据结构
        const customers = response.data?.data?.records;
        if (!Array.isArray(customers) || customers.length === 0) {
            logger.error('sku_kc接口未获取到有效客户数据');
            return res.status(404).json({ 
                success: false,
                error: '未获取到客户数据' 
            });
        }

        // 提取所有客户的customerCode并转为数值类型
        const customerCodes = customers.map(customer => {
            const code = parseInt(customer.customerCode, 10);
            if (isNaN(code)) {
                throw new Error(`无效的客户代码: ${customer.customerCode}`);
            }
            return code;
        });
        
        // 获取最大客户代码
        const maxCustomerCode = Math.max(...customerCodes);
        // 设置起始客户代码
        const baseCustomerCode = 1440001;
        
        // 生成从baseCustomerCode到maxCustomerCode的连续客户代码范围
        const customerCodeRange = Array.from(
            { length: maxCustomerCode - baseCustomerCode + 1 }, 
            (_, i) => baseCustomerCode + i
        );

        // 定义获取token的辅助函数
        const getToken = async (customerCode) => {
            const formData = new URLSearchParams();
            formData.append('customerCode', customerCode.toString());
            const tokenResponse = await instance.post('https://omp.xlwms.com/gateway/omp/customer/getTokenForOms', formData, {
                headers: {
                    ...config.externalApi.headers,
                    'Content-Type': 'application/x-www-form-urlencoded'
                },
                timeout: 15000
            });

            if (tokenResponse.data.code !== 200) {
                throw new Error(`获取token失败: ${tokenResponse.data.msg || '未知错误'}`);
            }
            return tokenResponse.data.data.token; // 直接返回token值
        };

        // 定义调用库存接口的辅助函数（使用用户指定的请求头和参数）
        const fetchStockData = async (customerCode, token) => {
            // 存储所有页的库存数据
            const allStockData = [];
            let currentPage = 1;
            let totalPages = 1;
            
            // 循环获取所有页的数据
            while (currentPage <= totalPages) {
                try {
                    const stockResponse = await instance.post('https://oms.xlwms.com/gateway/woms/stock/list', {
                        current: currentPage,
                        size: 200,
                        stockType: "",
                        whCodeList: "",
                        stockCountKind: "totalAmount",
                        startValue: "",
                        endValue: "",
                        categoryIdList: [],
                        isHideInventory: 0,
                        barcodeType: "sku",
                        barcode: "",
                        sku: ""
                    }, {
                        headers: {
                            "Accept": "application/json, text/plain, */*",
                            "Accept-Encoding": "gzip, deflate, br",
                            "Accept-Language": "zh-CN,zh;q=0.9",
                            "Authorization": `Bearer ${token}`, // 传入当前客户的token
                            "Connection": "keep-alive",
                            "Content-Type": "application/json;charset=UTF-8",
                            "Cookie": "language=cn; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22195604e5ae1a48-00b54a1894e4f5d-26021e51-2073600-195604e5ae2d00%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTk1NjA0ZTVhZTFhNDgtMDBiNTRhMTg5NGU0ZjVkLTI2MDIxZTUxLTIwNzM2MDAtMTk1NjA0ZTVhZTJkMDAifQ%3D%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%7D; version=prod; _hjSessionUser_3119560=eyJpZCI6Ijk4MGU4MDFmLTVkYzUtNTRlNy04NmU4LTQ5OTE1ODk5ZDZkMCIsImNyZWF0ZWQiOjE3NDEwNzc1MDMxMTYsImV4aXN0aW5nIjp0cnVlfQ==; _gid=GA1.2.1847758762.1741572024; _hjSession_3119560=eyJpZCI6IjMwMjQ1MzQ0LTQ5YjctNDcwMy1hOWYyLWExOGFhMWRmMjVhOSIsImMiOjE3NDE3NDU5NTIxMDIsInMiOjAsInIiOjAsInNiOjAsInNiOjF9; _ga_NRLS16EKKE=GS1.1.1741748917.38.0.1741748917.0.0.0; _ga=GA1.1.1880213018.1741077503; _ga_2HTV43T3DN=GS1.1.1741745951.33.1.1741748921.0.0.0; sidebarStatus=0; prod=always",
                            "Host": "oms.xlwms.com",
                            "Origin": "https://oms.xlwms.com",
                            "Referer": "https://oms.xlwms.com/stock",
                            "sec-ch-ua": "\"Google Chrome\";v=\"107\", \"Chromium\";v=\"107\", \"Not=A?Brand\";v=\"24\"",
                            "sec-ch-ua-mobile": "?0",
                            "sec-ch-ua-platform": "\"Windows\"",
                            "Sec-Fetch-Dest": "empty",
                            "Sec-Fetch-Mode": "cors",
                            "Sec-Fetch-Site": "same-origin",
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
                            "version": "prod"
                        },
                        timeout: 30000
                    });

                    if (stockResponse.data.code !== 200) {
                        throw new Error(`库存接口调用失败: ${stockResponse.data.msg || '未知错误'}`);
                    }

                    // 提取当前页的记录并添加到总结果中
                    const currentPageRecords = stockResponse.data.data.records.map(item => ({
                        sku名称: item.sku,
                        总库存: item.totalAmount,
                        可用库存: item.availableAmount,
                        锁定库存: item.lockAmount,
                        在途库存: item.transportAmount
                    }));
                    allStockData.push(...currentPageRecords);

                    // 更新总页数信息
                    totalPages = stockResponse.data.data.pages;
                    
                    // 打印进度信息
                    logger.info(`客户代码 ${customerCode}: 已获取第 ${currentPage}/${totalPages} 页数据，当前页记录数: ${currentPageRecords.length}`);
                    
                    // 准备获取下一页
                    currentPage++;
                    
                    // 为避免频繁请求，添加小延迟
                    if (currentPage <= totalPages) {
                        await new Promise(resolve => setTimeout(resolve, 500));
                    }
                } catch (pageError) {
                    logger.error(`客户代码 ${customerCode} 获取第 ${currentPage} 页数据失败: ${pageError.message}`);
                    // 出错时仍然增加页码，避免无限循环
                    currentPage++;
                }
            }
            
            return allStockData;
        };

        // 最终存储结果的数组
        const finalResults = [];

        // 遍历生成的客户代码范围：获取token -> 调用库存接口 -> 整理数据
        for (const code of customerCodeRange) {
            try {
                // 获取当前客户的token
                const token = await getToken(code);
                
                // 调用库存接口获取数据（包含所有页）
                const stockData = await fetchStockData(code, token);
                
                // 获取客户名称，如果找不到则使用代码作为备用
                const customerName = customerCodeToNameMap.get(code) || `未知客户(${code})`;
                
                // 整理需要存储的结构，使用客户名称而非代码
                finalResults.push({
                    客户名称: customerName,
                    客户代码: code, // 保留代码信息，但不作为主要标识
                    库存数据: stockData,
                    记录总数: stockData.length
                });
                
                logger.info(`客户 ${customerName} (代码: ${code}) 处理完成，共获取 ${stockData.length} 条库存记录`);
            } catch (error) {
                // 获取客户名称，如果找不到则使用代码作为备用
                const customerName = customerCodeToNameMap.get(code) || `未知客户(${code})`;
                
                logger.error(`客户 ${customerName} (代码: ${code}) 处理失败: ${error.message}`);
                finalResults.push({
                    客户名称: customerName,
                    客户代码: code,
                    错误信息: error.message,
                    库存数据: []
                });
            }
        }

        // 写入sku_kc.json文件（当前目录）
        const filePath = path.join(__dirname, 'sku_kc.json');
        await fs.promises.writeFile(filePath, JSON.stringify(finalResults, null, 2), 'utf-8');
        logger.info(`库存数据已成功写入: ${filePath}`);

        // 返回接口响应
        res.json({
            success: true,
            message: '数据处理完成，结果已存入',
            data: {
                processedCustomers: customerCodeRange.length
                // resultPath: filePath
            }
        });

    } catch (error) {
        logger.error(`sku_kc接口整体请求失败: ${error.stack}`);
        if (error.response) {
            res.status(error.response.status).json({
                success: false,
                error: error.response.data?.message || '外部接口请求失败'
            });
        } else {
            res.status(500).json({ 
                success: false,
                error: '服务器内部错误' 
            });
        }
    }
});
// 新增接口：获取备货详情
app.get('/get_beihuo', async (req, res) => {
    try {
        // 从查询参数中获取必要信息
        const { deliveryNo, customerCode, whCode, outboundWay = 1 } = req.query;
        
        // 验证必要参数
        if (!deliveryNo || !customerCode || !whCode) {
            return res.status(400).json({
                success: false,
                error: '参数错误',
                details: '缺少必要参数: deliveryNo, customerCode 或 whCode'
            });
        }

        // 构建请求URL
        const url = `https://omp.xlwms.com/gateway/omp/order/delivery/big/detailBySku?deliveryNo=${deliveryNo}&customerCode=${customerCode}&whCode=${whCode}&outboundWay=${outboundWay}`;

        // 发送请求到外部API
        const response = await instance.get(url, {
            headers: config.externalApi.headers,
            timeout: 15000
        });

        // 处理响应数据
        if (response.data && response.data.data) {
            // 假设返回的数据结构是 { success: true, data: { ... } }
            res.json({
                success: true,
                data: response.data.data
            });
        } else {
            logger.error('响应数据结构不完整，无法获取数据');
            res.status(500).json({
                success: false,
                error: '服务端错误',
                details: '响应数据结构不完整，无法获取所需信息'
            });
        }
    } catch (error) {
        // 错误处理
        if (error.response) {
            logger.error(`请求失败，状态码: ${error.response.status}，响应数据: ${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            logger.error('请求发送成功，但没有收到响应');
        } else {
            logger.error(`请求设置出错: ${error.message}`);
        }
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});
// 新增接口：获取备货分页数据并自动翻页
app.get('/get_beihuo1', async (req, res) => {
    try {
        // 从查询参数中获取必要信息，提供默认值
        const { 
            startTime = '2025-01-01 00:00:00', 
            endTime = new Date().toISOString().slice(0, 19).replace('T', ' ')
        } = req.query;

        // 构建请求URL
        const url = 'https://omp.xlwms.com/gateway/omp/order/delivery/big/page';
        
        // 存储所有分页数据
        const allData = [];
        let currentPage = 1;
        let totalPages = 1;
        
        // 循环获取所有分页数据
        while (currentPage <= totalPages) {
            // 构建请求参数
            const requestData = {
                timeType: "createTime",
                startTime,
                endTime,
                current: currentPage,
                size: 200,
                total: 0
            };
            
            // 发送POST请求到外部API
            const response = await instance.post(url, requestData, {
                headers: config.externalApi.headers,
                timeout: 15000
            });
            
            // 处理响应数据
            if (response.data && response.data.data && response.data.data.records) {
                // 提取所需字段并过滤状态为99的数据
                const filteredData = response.data.data.records
                    .filter(item => item.status === 30)  
                    .map(item => ({
                        whCodeName: item.whCode,
                        sourceNo: item.sourceNo,
                        customerName: item.customerName,
                        customerCode: item.customerCode,
                        logisticsChannelName: item.logisticsChannelName,
                        deliveryNo: item.deliveryNo
                    }));
                
                allData.push(...filteredData);
                
                // 更新总页数信息（注意：这里可能需要重新计算总页数，因为服务端可能返回未过滤的总数）
                totalPages = response.data.data.pages || 1;
                logger.info(`已获取第 ${currentPage} 页数据，共 ${totalPages} 页，过滤后数据量: ${filteredData.length}`);
                
                // 准备获取下一页
                currentPage++;
                
                // 如果不是最后一页，添加短暂延迟避免请求过于频繁
                if (currentPage <= totalPages) {
                    await new Promise(resolve => setTimeout(resolve, 500));
                }
            } else {
                logger.error('响应数据结构不完整，无法获取分页信息');
                break;
            }
        }
        
        // 返回所有数据
        res.json({
            success: true,
            data: allData,
            total: allData.length
        });
        
    } catch (error) {
        // 错误处理
        if (error.response) {
            logger.error(`请求失败，状态码: ${error.response.status}，响应数据: ${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            logger.error('请求发送成功，但没有收到响应');
        } else {
            logger.error(`请求设置出错: ${error.message}`);
        }
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    }
});


const uploadStorage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, 'ERROR_DATA/');
    },
    filename: function (req, file, cb) {
        const id = req.body.id;
        const type = req.body.type;
        
        if (!id || !type) {
            cb(new Error('缺少必要参数: id 或 type'), null);
            return;
        }
        
        if (type !== '解决' && type !== '提出' && type !== '跟进') {
            cb(new Error('type参数有误'), null);
            return;
        }
        
        const ext = path.extname(file.originalname);
        
        // 获取当前时间并格式化为 年月日时分秒毫秒
        const now = new Date();
        const formattedDate = `${now.getFullYear()}年${(now.getMonth() + 1).toString().padStart(2, '0')}月${now.getDate().toString().padStart(2, '0')}日${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}:${now.getMilliseconds().toString().padStart(3, '0')}`;
        
        // 生成4位随机数
        const randomNum = Math.floor(1000 + Math.random() * 9000);
        
        // 构建文件名：id-类型-年月日时分秒毫秒-随机数.扩展名
        const fileName = `${id}-${type}-${formattedDate}-${randomNum}${ext}`;
        
        cb(null, fileName);
    }
});

// 创建multer实例
const upload1 = multer({ 
  storage: uploadStorage,
    limits: {
        fileSize: 1024 * 1024 * 50 // 限制文件大小为50MB
    }
});

// 新增接口：上传附件
app.post('/uploadFile', upload1.array('files'), async (req, res) => {
    try {
        // 检查是否上传了文件
        if (!req.files || req.files.length === 0) {
            return res.status(400).json({
                success: false,
                error: '未上传任何文件'
            });
        }
        
        // 构建文件URL数组
        const fileUrls = req.files.map(file => {
            return `/ERROR_DATA/${file.filename}`;
        });
        
        // 返回成功响应
        res.json({
            success: true,
            message: '文件上传成功',
            files: fileUrls,
            count: req.files.length
        });
        
    } catch (error) {
        // 错误处理
        logger.error(`文件上传失败: ${error.message}`);
        res.status(500).json({
            success: false,
            error: '文件上传失败',
            details: error.message
        });
    }
});

// 新增接口：根据ID打包下载文件 (修改为POST方法)
app.post('/downloadFiles', express.json(), async (req, res) => {
    try {
        // 从请求体中获取ID
        const id = req.body.id;
        
        // 检查ID是否存在
        if (!id) {
            return res.status(400).json({
                success: false,
                error: '缺少必要参数: id'
            });
        }
        
        const directoryPath = path.join(__dirname, 'ERROR_DATA');
        
        // 检查目录是否存在
        if (!fs.existsSync(directoryPath)) {
            return res.status(404).json({
                success: false,
                error: '文件目录不存在'
            });
        }
        
        // 读取目录中的所有文件
        const files = fs.readdirSync(directoryPath);
        
        // 筛选出与ID匹配的文件
        const matchingFiles = files.filter(file => file.startsWith(`${id}-`));
        
        if (matchingFiles.length === 0) {
            return res.status(404).json({
                success: false,
                error: `未找到ID为${id}的文件`
            });
        }
        
        // 创建ZIP文件
        const archive = archiver('zip', {
            zlib: { level: 9 } // 压缩级别
        });
        
        // 设置响应头
        res.attachment(`files_${id}.zip`);
        archive.pipe(res);
        
        // 将匹配的文件添加到ZIP文件中
        matchingFiles.forEach(file => {
            const filePath = path.join(directoryPath, file);
            archive.file(filePath, { name: file });
        });
        
        // 完成ZIP文件
        archive.finalize();
        
    } catch (error) {
        logger.error(`文件下载失败: ${error.message}`);
        res.status(500).json({
            success: false,
            error: '文件下载失败',
            details: error.message
        });
    }
});
    
// 新增接口：获取物流信息
app.get('/get_uniuni', async (req, res) => {
    try {
        // 从查询参数中获取ID
        const id = req.query.id;
        
        // 检查ID是否存在
        if (!id) {
            return res.status(400).json({
                success: false,
                error: '缺少必要参数: id'
            });
        }
        
        // 构建请求URL
        const url = `https://delivery-api.uniuni.ca/cargo/trackinguniuninew?id=${id}&key=SMq45nJhQuNR3WHsJA6N`;
        
        // 设置请求头
        const headers = {
            'Host': 'delivery-api.uniuni.ca',
            'Connection': 'keep-alive',
            'sec-ch-ua-platform': '"Windows"',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'sec-ch-ua': '"Chromium";v="136", "Microsoft Edge";v="136", "Not.A/Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'Origin': 'https://www.uniuni.com',
            'Sec-Fetch-Site': 'cross-site',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': 'https://www.uniuni.com/',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6'
        };
        
        // 发送GET请求
        const response = await axios.get(url, { headers });
        
        // 返回响应数据
        res.json({
            success: true,
            data: response.data
        });
        
    } catch (error) {
        logger.error(`获取物流信息失败: ${error.message}`);
        
        // 处理不同类型的错误
        if (error.response) {
            // 服务器返回了错误状态码
            res.status(error.response.status).json({
                success: false,
                error: '请求失败',
                details: error.response.data
            });
        } else if (error.request) {
            // 请求已发送，但没有收到响应
            res.status(500).json({
                success: false,
                error: '没有收到服务器响应',
                details: error.message
            });
        } else {
            // 发生了其他错误
            res.status(500).json({
                success: false,
                error: '请求处理失败',
                details: error.message
            });
        }
    }
});

// 新增接口：获取USPS物流信息
app.get('/get_usps1', async (req, res) => {
    try {
        // 从查询参数中获取追踪号码
        const id = req.query.id;
        
        // 检查追踪号码是否存在
        if (!id) {
            return res.status(400).json({
                success: false,
                error: '缺少必要参数: id'
            });
        }
        
        // 构建请求URL
        const url = `https://tools.usps.com/go/TrackConfirmAction?qtc_tLabels1=${id}`;
        
        // 设置请求头
        const headers = {
            'Host': 'tools.usps.com',
            'Connection': 'keep-alive',
            'sec-ch-ua-platform': '"Windows"',
            'CDN-Loop': 'akamai;v=1.0;c=1',
            'Akamai-Origin-Hop': '2',
            'sec-ch-ua': '"Chromium";v="136", "Microsoft Edge";v="136", "Not.A/Brand";v="99"',
            'Client-IP': '23.59.160.169',
            'X-jFuguZWB-a': 'Ewtz-E4IXgM-qH96elby81J4WPMgSgxsLTbYYweA9YrlyNEvBgBgIFWS5jp6nyiWAK-Pu8Yen9WYkq=UbK5q85Df26Og_So472hIMpbztuJKzfzeZ5ED_U-aSASbPvuVOmWS4FxIOkeWuZX4UFbWzMv-dHMsHv-BpbtNmoPDBCVUUJDmCG52WCbq7zmZIq0gWyvoX0MBgvbhw0eIA4C75PiXNBtnnlYhqHh=GsIUzuQat62q6HhcUYocncoRJX7VUR5m_85o-n21lBZohHbPqlMRK1Xb889crPxG40hmm-1hiXCGjQDEf1KdWbrvYD8qbZ0572gdgAg8Y0xyigA=onDK-x5sU8zwQNjag1iBl-xcTqm4fo9Kig5q0OCorRMqMJcy2XgV8D9k-K0FAmujz62V6pcbNSev5iv9WWLayWH91mhm7CPoBrDdTqeFqVo4=_zxQy45rGQ=Bdq6krfJR0H21IH8ZFFC8vOWec4Gdrsk2rYje_R7QwrMcV6tkaPCTT21JbzvQJVPY0cR5H4BFOISkojgfTVEKJFQnGv6iTbQ-kYv1Ba7j9NMVGGzowIa=RCCyjdBrzEiUBi2Ejh7J6oqsCedLlzxyUvgF85M5jcqXQqxcwW=HYfYvabebVvN2X-JQSjQXb0Yx7-d8gMpbht2aDevQ==oNa66BcNqdzeWO27JXifrbwH7CP7R71rG7n1omg=DbqDqc5UcuCanVaY879guFKYq1dE74XjLU4mY9_2bv0UWw9Fj5EN9b9Vp0eU9TMmLAeRf021zVhu127XiyIKg44w5-pX==KtAbprIFFTVh0oae4c-dKCQwy8_qc4s=WiRuPvW_YHMjodfyI8ijIcmvht-wtvk7Yg5chSn8YJBohIQ4g=a9MYk2I=5FnF6TG59v0Xcfsm-cvjh18wGyFn1CQCHQuYhxiXIS9eMwpEv4TyDSU59RVbC26u-7jEZqpuQvT07RYG9op7f0y=xnbFh_oAf7H8BzaH=ciZ=RzVA=E-ub9AbANd2T_P2xIfaeo-B-OC_g467Pzs4lb=2xPottg0S4N4pjz_UORRCTyMDy0vktVD47pGyVbLrT9KU2urwnCeChCZaMfnJGvzeu9ZtfYJznqMeza1UVM1h6wnMM_xBCtV2s70UmuMicUy6iC8XWh=YNxZSD5-BxrJf65rcQU6QpcGILAZPDQpctPulsUXXSjrTy6=aSjfOWgr0PVSq1GGqM0IOqNYFvBii9BIB0UfzuH-Vb1mmnv2rVBnvAnSj_PYvtQdJGWsUY8G1_jSwCEVe_x7_wDuvidFftM1ORKsZ1sYHee1WMfQAZ9DgVO7O1JYlXyidHl5-Q7SRV8TGcYwqCvaJUR86mz6sRDAz_-C20y-YbuWpTuofI-kh1oolpMVFa7WKkUiDqq_cRg87TQoyRldnklKs1TcHER9T2u6Ayn7YG6xM=FZcm0iNN-S=1a=pK1dgQb5zTeL5HpjFn_gEmgnCus5f_jlxz_7J=SXOMzi8_42OmracouCDmFyHnpHmz5xluzljN_1dB7S_Tc9Upt-TRKKY4VmLh5CkTHNfe57OUjHb_i1_F05cSv8vf7qn6oq9SNl-_AH4SZzsn45El94_PJwlA=x=017L5MNuqNcFOxlkewvkJEFWjD98ftvL94NdzS4chnwKBPe0iFNAMbWQP0cYK8Ppi_1uKao617VeD7TR4hXFG4X-AxhfuLOf-2y9cPyGCaQcCtrL_oylc4=9MBtGLu=sOA5JJ=4uKuktZmC_Gvs-4qDQpZPMGPceewx8q1tFryp5n9qMR48t4G7=tco7ab1JnqIl1HsOjI4ff6uFngk8cKVUoSOa9nKmnRolH4rP_xjMmSDWssAqbfdKx94rglDs1ZYP68R2B=COPCdBvqJwJv9h7gtW4jdwiltxVtTpcqxUMU9kRq9OcOO_o9-MntqyoQmHydr7_kPIehPtEw8cnLy-dYhOnonTu7e5EOP6TkiTEu=fFblacsn9YDYctlGm_N_XrZCZrH-5iXeEzZWULwNsFc1W7pNkYDiysr2gm=0JdmFj=-76Z96BwlcypOFJ4Axak9=PNRRLY1lGeziOQdLLiXh2VfIbHH7D17yPjm_IwZ44bhEjIeal98T17VAcz0508qTjyafZP-uMyWKVkb0-eMcR2Wy_4ZbEqA=RXDip2668QCzoxaLSbl9sJxzLFiignSiO0YZrMv--AhDE',
            'X-Forwarded-For': '2409:8a55:38f9:d930:c964:590b:46d8:ad84, 184.51.102.101, 56.0.33.9, 34.128.178.207',
            'sec-ch-ua-mobile': '?0',
            'X-jFuguZWB-b': '-8kihlv',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'X-Cloud-Trace-Context': 'c25226bb596cb61b6ea7cc4a5971363a/11822940093790797107',
            'X-jFuguZWB-d': 'ABaAhACBAKCBgQGAAYAQgICCAKIAwAGABJhCgAyVEIIhkIDJCAALiX02lXVJXf____-H0zuoAvS_YOMBGjuKDADaaMjwWBI',
            'ISTL-INFINITE-LOOP': '1',
            'Cache-Control': 'no-cache, max-age=0',
            'X-Forwarded-Proto': 'https',
            'X-jFuguZWB-f': 'A37STYeXAQAAujJvjNR-VRP3m6Py0r2K1TEWhZ9vciE875IS0Wl_xTLNmvmDABdOs3nAfwAAQHcAAAAA',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Pragma': 'no-cache',
            'NS-Client-IP': '2409:8a55:38f9:d930:c964:590b:46d8:ad84',
            'Upgrade-Insecure-Requests': '1',
            'HTTP-X-EC-GEODATA': 'geo_asnum=20940,geo_city=KOWLOON,geo_continent=AS,geo_country=HK,geo_latitude=22.32,geo_longitude=114.18,geo_postal_code=,geo_region=,wireless=false',
            'X-jFuguZWB-z': 'q',
            'ISTL-REFERER': 'https://zh-tools.usps.com/',
            'X-Akamai-CONFIG-LOG-DETAIL': 'true',
            'X-jFuguZWB-c': 'AGCPSoeXAQAAGTqj5jOabX2_rHCvOl2p0zYPshw6VAbbTVNFQwuJfTaVdUld',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
            'X-BM-HA': '1~28~6853ca31~d15673d6974c52ed50e03f83201d9f3557a7792485bfd7af034d5924cabad7f9',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': 'https://tools.usps.com/go/TrackConfirmAction?qtc_tLabels1=9200190388744003922085',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Cookie': '_gcl_au=1.1.1699940676.1746673570; _fbp=fb.1.1748501179088.622152482401878541; _scid=LMNLKbTBDDA5NPqvrSehIkjYB6jbyfpD; _scid_r=LMNLKbTBDDA5NPqvrSehIkjYB6jbyfpD; _rdt_uuid=1748501179379.4944dab7-7954-4d80-b5c8-c38d85381c2c; _dpm_id.340b=5bb70d87-7e5e-464e-bf55-4187dde09ac2.1748501179.1.1748501179.1748501179.9eb99de1-6e3b-4b9b-9100-24b3efcb01ef; sm_uuid=1748501721537; _uetvid=a0a935803c5811f0870307915214637d; o59a9A4Gx=BbVYUrSWAQAA7FCoj5FzMg8jjtJ0r2yT8lVUJlr2I1qBkDWq-2WORdEb43GEJBdOs3nAfwAAQHcAAAAAJAmKVTj3t4D1dM2wdk4H5w|1|1|5bb8755d81ac6c2adf2b52654c26649bbf6e7566; TLTSID=da9ee014d78016638b0600e0ed96ae55; NSC_u.tt1_443=ffffffff2188acde45525d5f4f58455e445a4a42378b; ak_bmsc=19364ECE091963434FE0916A1CC02763~000000000000000000000000000000~YAAQR0dYaJYoNF+XAQAA7wn5hhyBGa92EfKmyqmaSB0UccZCIeGVhsLKDUoHok41PWzrL0pWbyQde97QPCmdfARFhTl2+NgeSEAuSG6Vm/+MF3wCfdfmllIbsd1doJGxkVLsEfRC2NhAyr3yiNWr/H+9SQsIhnzOih83J1tAA+XmUa9JCziUXT3b64iS1+DY0qpIaLOWm34LCPNwAaacIM2+VXHGqaIrplwZU8ZLoOpgO9dUQIsf2+4PhHQJUINve+kSYqj1Q4ej1wJZwxHiYyVMMoD+g8DetfO5hGS82AMHyLHPUrC5yHde7ALigsLcYokPVqmjM0M1csvvH0CyuoHEGdQs/LMeziCMeoAmYePVRHPpVHQD2dx+pJW1GNRANV2vYxB8e+ILN9HYPvkG1yhJtfHaZFKVbrYuETgacNf4xg==; NSC_uppmt-hp=ffffffff3b462a3d45525d5f4f58455e445a4a4212d3; NSC_uppmt-usvf-ofx=ffffffff3b22378a45525d5f4f58455e445a4a4212d3; mab_usps=90; tmab_usps=11; _gid=GA1.2.1791528809.1750316162; w3IsGuY1=BWiRPYeXAQAA9gI-q6P3MU3SldokVb0Bu4b9aUWbF1RDcZ3ku_HKeuCoGahBJBdOs3nAfwAAQHcAAAAAJAmKVTj52TDJZFkLRtithA==; JSESSIONID=0000v9r_ITv2dPWYxDYYMYbeqh2:1bbe7u0v4; kampyleUserSession=1750320858109; kampyleUserSessionsCount=14; kampyleSessionPageCounter=1; _ga_QM3XHZ2B95=GS2.1.s1750321489$o3$g1$t1750321534$j15$l0$h0; _dc_gtm_UA-80133954-3=1; _ga=GA1.1.910962779.1746673571; _ga_3NXP3C8S9V=GS2.1.s1750316161$o46$g1$t1750321714$j26$l0$h0; bm_sv=2E3A1BB71D6FF36EB6F1770189BB35B7~YAAQZWYzuLUbRGCXAQAAA9NNhxziEmfs2/hRQ6sNN8nBJGbCG5w5gVFbewHLAjSGeyBbo1IEBwPHlf6R0EJ0G4JuaMfMxKpa9iKSrqDPC9Hhl799cxIYUBFDmShiBPvZXKBE1LmnBncMj4im1J5KJhbmvGRnUc616zoyUlx3EzYQU0b9s16xtux6GmBYrZQznTbpi8uDAVf5RsZqmpU7PPS87GyVby7lyZcAXN0SYxCXEvNP98bY7u8lVcrNTIlU~1; _ga_CSLL4ZEK4L=GS2.1.s1750316162$o45$g1$t1750321715$j25$l0$h0'
        };
        
        // 发送GET请求
        const response = await axios.get(url, { headers });
        
        // 返回响应数据
        res.json({
            success: true,
            data: response.data
        });
        
    } catch (error) {
        logger.error(`获取USPS物流信息失败: ${error.message}`);
        
        // 处理不同类型的错误
        if (error.response) {
            // 服务器返回了错误状态码
            res.status(error.response.status).json({
                success: false,
                error: '请求失败',
                details: error.response.data
            });
        } else if (error.request) {
            // 请求已发送，但没有收到响应
            res.status(500).json({
                success: false,
                error: '没有收到服务器响应',
                details: error.message
            });
        } else {
            // 发生了其他错误
            res.status(500).json({
                success: false,
                error: '请求处理失败',
                details: error.message
            });
        }
    }
});    
// 新增接口：通过toolstip.cn获取USPS物流信息（使用axios）
app.get('/get_usps', async (req, res) => {
    // 从查询参数中获取追踪号码
    const trackingNumber = req.query.number;
    
    // 检查追踪号码是否存在
    if (!trackingNumber) {
        return res.status(400).json({
            success: false,
            error: '缺少必要参数: number'
        });
    }

    // 设置请求头
    const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0',
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Referer': 'https://www.toolstip.cn/tracking/express-usps.html',
        'Origin': 'https://www.toolstip.cn',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
    };

    // 创建axios实例（用于共享cookie，模拟会话）
    const instance = axios.create({
        withCredentials: true, // 关键：允许跨域请求携带cookie
        headers: headers
    });

    try {
        // 1. 先访问Referer页面，建立会话（获取初始cookie）
        console.log("开始访问Referer页面，建立会话...");
        const refererRes = await instance.get('https://www.toolstip.cn/tracking/express-usps.html');
        if (refererRes.status !== 200) {
            throw new Error(`访问Referer页面失败，状态码：${refererRes.status}`);
        }
        console.log("Referer页面访问成功，会话已建立");

        // 2. 发送跟踪查询POST请求
        console.log(`开始查询物流单号：${trackingNumber}`);
        const url = 'https://www.toolstip.cn/tracking/gettrace.php';
        const postData = new URLSearchParams(); // 处理表单数据
        postData.append('number', trackingNumber);
        postData.append('express', 'usps');
        postData.append('lang', 'cn');

        const response = await instance.post(url, postData, {
            headers: {
                // 覆盖Content-Type（URLSearchParams会自动处理，但显式指定更稳妥）
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
            }
        });

        // 验证响应状态
        if (response.status !== 200) {
            throw new Error(`查询请求失败，状态码：${response.status}`);
        }

        // 尝试解析JSON响应（axios会自动解析，但做一层验证）
        if (typeof response.data !== 'object') {
            throw new Error(`响应格式异常，原始内容：${JSON.stringify(response.data).substring(0, 500)}`);
        }

        // 返回成功结果
        res.json({
            success: true,
            data: response.data
        });

    } catch (error) {
        console.error(`获取USPS物流信息失败：${error.message}`);
        
        // 处理不同类型的错误
        if (error.response) {
            // 服务器返回错误响应
            res.status(error.response.status || 500).json({
                success: false,
                error: '请求失败',
                statusCode: error.response.status,
                details: error.response.data ? 
                    (typeof error.response.data === 'string' 
                        ? error.response.data.substring(0, 500) 
                        : JSON.stringify(error.response.data).substring(0, 500))
                    : '无响应内容'
            });
        } else if (error.request) {
            // 请求已发送但无响应
            res.status(504).json({
                success: false,
                error: '请求超时，未收到服务器响应',
                details: error.message
            });
        } else {
            // 其他错误（如参数错误、解析错误）
            res.status(500).json({
                success: false,
                error: '服务器处理失败',
                details: error.message
            });
        }
    } finally {
        // 不需要手动关闭会话（axios实例无显式关闭方法，由GC自动处理）
        console.log("请求流程结束");
    }
});

// 新增接口：获取Gofo物流信息
app.post('/get_gofo', async (req, res) => {
    try {
        // 从查询参数中获取追踪号码
        const id = req.query.id;
        
        // 检查追踪号码是否存在
        if (!id) {
            return res.status(400).json({
                success: false,
                error: '缺少必要参数: id'
            });
        }
        
        // 构建请求URL
        const url = `https://www.gofoexpress.com/api/track/query?numberList=${id}`;
        
        // 设置请求头
        const headers = {
            'Host': 'www.gofoexpress.com',
            'Connection': 'keep-alive',
            'Content-Length': '0',
            'sec-ch-ua-platform': '"Windows"',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'sec-ch-ua': '"Chromium";v="136", "Microsoft Edge";v="136", "Not.A/Brand";v="99"',
            'Content-Type': 'application/json',
            'sec-ch-ua-mobile': '?0',
            'Origin': 'https://www.gofoexpress.com',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': `https://www.gofoexpress.com/tracking.html?searchID=${id}`,
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Cookie': '_ga=GA1.1.2121317864.1749175889; cookieControlPrefs=%5B%22preferences%22%2C%22analytics%22%2C%22marketing'
        };
        
        // 发送POST请求
        const response = await axios.post(url, {}, { headers });
        
        // 返回响应数据
        res.json({
            success: true,
            data: response.data
        });
        
    } catch (error) {
        logger.error(`获取Gofo物流信息失败: ${error.message}`);
        
        // 处理不同类型的错误
        if (error.response) {
            // 服务器返回了错误状态码
            res.status(error.response.status).json({
                success: false,
                error: '请求失败',
                details: error.response.data
            });
        } else if (error.request) {
            // 请求已发送，但没有收到响应
            res.status(500).json({
                success: false,
                error: '没有收到服务器响应',
                details: error.message
            });
        } else {
            // 发生了其他错误
            res.status(500).json({
                success: false,
                error: '请求处理失败',
                details: error.message
            });
        }
    }
});    
// 新增接口：获取UPS物流信息
app.post('/get_ups', async (req, res) => {
    try {
        // 从请求体中获取追踪号码
        const { id } = req.body;
        
        // 检查追踪号码是否存在
        if (!id || id.length === 0) {
            return res.status(400).json({
                success: false,
                error: '缺少必要参数: id'
            });
        }
        
        // 构建请求URL
        const url = 'https://webapis.ups.com/track/api/Track/GetStatus?loc=en_US';
        
        // 设置必要的请求头（移除了硬编码的临时值）
           const headers = {
            'Host': 'webapis.ups.com',
            'Connection': 'keep-alive',
            'Content-Length': '121',
            'sec-ch-ua-platform': '"Windows"',
            'X-XSRF-TOKEN': 'CfDJ8Jcj9GhlwkdBikuRYzfhrpIDvMttc-XtvQK1o0ZLnYLbNkLMlzsxF9km5UwShra2bmqs7IZLkqlAVK_4aickjQHmn4OlHzlBGBesm9GcZ63Q9J3ciQIoCs_AdoE0fKedoYAXi3jOW8L5rEYz5cMpHMw',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
            'Accept': 'application/json, text/plain, */*',
            'sec-ch-ua': '"Chromium";v="136", "Microsoft Edge";v="136", "Not.A/Brand";v="99"',
            'Content-Type': 'application/json',
            'sec-ch-ua-mobile': '?0',
            'Origin': 'https://www.ups.com',
            'Sec-Fetch-Site': 'same-site',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Cookie': 'sharedsession=34e1ee53-940f-4350-a485-7124f1e6f44c:m; CONSENTMGR=consent:true%7Cts:1747030439480; ups_language_preference=en_US; _gcl_au=1.1.1713466840.1747030441; _fbp=fb.1.1747030441527.263143211701298949; _ga=GA1.1.1446792532.1747030442; jedi_loc=eyJibG9ja2VkIjp0cnVlLCJjb29yZGluYXRlcyI6WzIyLjY2MzYzNjQ0ODQ5MjA1NiwxMTMuOTk1OTcwODk5MDc1NzNdfQ%3D%3D; X-CSRF-TOKEN=CfDJ8Jcj9GhlwkdBikuRYzfhrpKWncIUAdgsUI8dP5VFtHMc6KbL9FlSHubfefFKbRKKVwsR8jAZeIrCDok2VC9G-DUY3da9L6HoCZzov1MP2wJczkPO-rGbCelNyZ5ZLXjSWEDMIR_kVxglB6XpRhYgFO4; PIM-SESSION-ID=Bsss7LSq74kjA4UR; AMCVS_036784BD57A8BB277F000101%40AdobeOrg=1; AMCV_036784BD57A8BB277F000101%40AdobeOrg=179643557%7CMCIDTS%7C20265%7CMCMID%7C51468084297242637414028894696903550778%7CMCAAMLH-1751447820%7C11%7CMCAAMB-1751447820%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1750850220s%7CNONE%7CvVersion%7C5.5.0; at_check=true; s_vnc365=1782379021740%26vn%3D12; s_tslv=1750843021741; s_cc=true; loc_session=MTc1MDg0MzAyMjg2Ng%3D%3D; _uetsid=9dc49310500911f09f425f40e5b898fb; _uetvid=4caed2302ef811f0bd53db6892f4c353; mbox=PC#61c9d76101654321954738f70ab685cf.32_0#1814087827|session#42e7c2417bfa4f0098bc306305768293#1750844887; utag_main=v_id:0196c320f62d00205df423e532980507d004f07500bd0$_sn:12$_se:7%3Bexp-session$_ss:0%3Bexp-session$_st:1750844827283%3Bexp-session$vapi_domain:ups.com$ses_id:1750843020822%3Bexp-session$_pn:1%3Bexp-session$sr_sample_user:undefined%3Bexp-session$bingSegment:NoSegment$googleSegment:NoSegment$qualtricsSegment:NoSegment$ttd_uuid:5d20117c-92bd-4110-b2f8-8bf02113f4fc%3Bexp-session$cms_105:1%3Bexp-session$_storepreviouspageids:tracking%2FtrackWeb%2Ftra(3det).html%3Bexp-1750846627291$_prevpageid:tracking%2FtrackWeb%2Ftra(3det).html%3Bexp-1750846624762$_prevpage:ups%3Aus%3Aen%3Atrack%3Bexp-1750846627290$googleTimeout:Y$bingTimeout:Y; s_nr30=1750843027302-Repeat; bm_so=03D62383A7AA304EB5859A6AC031B74FF8D2A5D443B3C395CC31CBB624513F04~YAAQz7khF5hXPYGXAQAA5flgpgQ97M5WQZBAo3Czpn2fJRvdlvHREg6aavveV7aVc6h1aClo82J6r54iju/uNu/vkDAcBLY43jCPudbSkloGqbaCeHnhRomNiZO8vMzFdZK+IeBzt9tq1ZnvBroHcTMjTB1sawtwcUvcRhIspvHhIj+Khv5b/0J2BofWCr1nQwL3nO1ezFiJcM/nJ48wMUCpfAFZGszytGdRxduejYA/dfDWdbGeGM3FhbeR+b0cD65+3x6IAyKSIaeQLfmW/EImSmB24Fvp/iQ/yGSljreDQ80pY+dBETudl93sxqqws3A8pOS3QLW5fF3lJ2rzPiKHxhvgq4EHZyKIPrsZRngPv81lKlqjAGgB4wounEi4sARVYWV5IKEv6c0nUPLCC0jFOfEjPG+KTY6qnpCc1GHeB08ypvtI3nKswnz4O+exQ9dAXAH1U42PGC7dMGOlARTaZSRkBMrM+tCsxPC6rcgNt3d7gw==; bm_lso=03D62383A7AA304EB5859A6AC031B74FF8D2A5D443B3C395CC31CBB624513F04~YAAQz7khF5hXPYGXAQAA5flgpgQ97M5WQZBAo3Czpn2fJRvdlvHREg6aavveV7aVc6h1aClo82J6r54iju/uNu/vkDAcBLY43jCPudbSkloGqbaCeHnhRomNiZO8vMzFdZK+IeBzt9tq1ZnvBroHcTMjTB1sawtwcUvcRhIspvHhIj+Khv5b/0J2BofWCr1nQwL3nO1ezFiJcM/nJ48wMUCpfAFZGszytGdRxduejYA/dfDWdbGeGM3FhbeR+b0cD65+3x6IAyKSIaeQLfmW/EImSmB24Fvp/iQ/yGSljreDQ80pY+dBETudl93sxqqws3A8pOS3QLW5fF3lJ2rzPiKHxhvgq4EHZyKIPrsZRngPv81lKlqjAGgB4wounEi4sARVYWV5IKEv6c0nUPLCC0jFOfEjPG+KTY6qnpCc1GHeB08ypvtI3nKswnz4O+exQ9dAXAH1U42PGC7dMGOlARTaZSRkBMrM+tCsxPC6rcgNt3d7gw==^1750843065957; X-XSRF-TOKEN-ST=CfDJ8Jcj9GhlwkdBikuRYzfhrpIDvMttc-XtvQK1o0ZLnYLbNkLMlzsxF9km5UwShra2bmqs7IZLkqlAVK_4aickjQHmn4OlHzlBGBesm9GcZ63Q9J3ciQIoCs_AdoE0fKedoYAXi3jOW8L5rEYz5cMpHMw; bm_s=YAAQz7khF7lXPYGXAQAAQhphpgMtQx9LznofDhIQXHd052U/mBOK+CTQ7Rduvx8xjwpQyDHJmTSgy/fHnpqpp5JXwnE4sBXncP4v25W1Z/PrYFPOIiU4Y1qNrArxnUq1f6HMCsjvHOmjainaXChBVpTxsu+LWGKBxQbBnJIegLwXY++FkSO8CcEtCENMM9awF1Tva2s7hw2Q9Uc5BMtafHaSuxq4wyDuaAkISAqvxr1d26wFOh5ALxzc7qRXwpQs1CzuTknSGUVl5N65W+IbajFDFRcfHIVlBRBtH+IEVDwht7akwm904u1yuhmyxwidsu0JSu+CIywTol2rakNJrwJ5b9ANsymp1dqIBYz3BNEI3OVFpsKC7cZo1MponPCLnIyVkYJ9DFvn1IYbwk4PrdmZh8AxXoKb2WPqdF1bO+GEd90LPRGwfEriY8ryKj4ozC6wB28VfJI0SgCKtGxREMZyRy90awofR50RYvD4/7DrPgHYI1DbtYt3CxEJuluK5nJw4ZdEEIOMLFcdFExKYuIXZ3Rxb8PKNOagq5MZcQlUjyqrawXvJg+2WDWbo9tyQ9Wz1Si39YTRQ19mPv4YDM27aRbl4UYvCm8kazb2tTBwvu/wIJtcrEMpFgODq1AOY1RMtXlzPh+ULHW8fzkj; _abck=A17719F53F578D60E0E93B5207FF5110~0~YAAQV2YzuEk9TYuXAQAAN+pnpg5lH4gXO+2H1xIVd0cNagJYi/F3JIAWbGNnhvTl6UNiJk7iPr1wbn16wgNHN2OEI7mkig7C+QRgZ9F+xsaRj+sy9E8G4ncE2v1BviXM/cHwpvnrLIhG6eA+aJ4+hVOnQT6V/76ZsbPQ/nKya9uNw8VbkpAYZdQkR/b0XEPx4B5AMkBfV6F2cUCa7/zSuE7i9ZllGy+cYdwtqqncL8nzgBd0+Kntk6ha8sWoB1o8Ktjt98pZDMZ5k2qu+ZKNI4qVVhMEKO9Js5Zaazz3wiPRd7URhCOGUM1Kpr+nMFNZCri1BWAcl+Ssf85FD0zv/aAru1t3x4KLU4ORfoH0s1eZMJ1FJdgp1T7ctEJWEoV737xj/7MjY06USpBGcRL2X/QULI+l/nDDBkImb6ALjsEkqox31haJzIoSCr8taA8hwqzRa/CsCQ/q+j1e8+XmCcsxg9xFzeFHv6rARpO3uEVqQVeXjLMt7pv8RTj7yJb/DGPZTgaepvn5kA8yD6QCIZL0mCtt2zfO1viygHJOmvH/PIlHtiit2gAZ3bAe0iYnOb3tYvTMDq+68+l6vXMH72T3wo/zODYZMJMD3Iw/LYnqQxGW5mI/Swm/X+/nKSgI5/W8Ym+RBHrWVPynM7JIlbbJZ3MilgNGsopw62CdkVUrNtnvbek1JQm6ec8eq2BcrrAD9riScmVQSIYzb5Kxlz+LLFB9Cvp/2I8lYC5L0IpAq2jXf4eOdhOIOxCC9GX0RDuK2bfuRzKAVopfO013FSggGc0SNFKdzUkdu30f~-1~||0||~-1; _ga_13B5QB73DW=GS2.1.s1750845242$o16$g0$t1750845242$j60$l0$h0'
        };
        
        // 构建符合UPS API要求的请求体
        const requestBody = {
            "Locale": "en_US",
            "TrackingNumber": [id],
            "isBarcodeScanned": false,
            "Requester": "quic",
            "returnToValue": ""
        };
        
        // 发送POST请求到UPS API
        const response = await axios.post(url, requestBody, { headers });
        
        // 返回响应数据
        res.json({
            success: true,
            data: response.data
        });
        
    } catch (error) {
        console.error(`获取UPS物流信息失败: ${error.message}`);
        
        // 处理不同类型的错误
        if (error.response) {
            // 服务器返回了错误状态码
            res.status(error.response.status).json({
                success: false,
                error: '请求失败',
                details: error.response.data
            });
        } else if (error.request) {
            // 请求已发送，但没有收到响应
            res.status(500).json({
                success: false,
                error: '没有收到服务器响应',
                details: error.message
            });
        } else {
            // 发生了其他错误
            res.status(500).json({
                success: false,
                error: '请求处理失败',
                details: error.message
            });
        }
    }
});
// 新增接口：获取物流信息
app.get('/get_amazon', async (req, res) => {
    try {
        // 从查询参数中获取ID
        const id = req.query.id;
        
        // 检查ID是否存在
        if (!id) {
            return res.status(400).json({
                success: false,
                error: '缺少必要参数: id'
            });
        }
        
        // 构建请求URL
        const url = `https://track.amazon.com/api/tracker/${id}`;
        
        // 设置请求头
        const headers = {
            // 'Host': 'delivery-api.uniuni.ca',
            // 'Connection': 'keep-alive',
            // 'sec-ch-ua-platform': '"Windows"',
            // 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
            // 'Accept': 'application/json, text/javascript, */*; q=0.01',
            // 'sec-ch-ua': '"Chromium";v="136", "Microsoft Edge";v="136", "Not.A/Brand";v="99"',
            // 'sec-ch-ua-mobile': '?0',
            // 'Origin': 'https://www.uniuni.com',
            // 'Sec-Fetch-Site': 'cross-site',
            // 'Sec-Fetch-Mode': 'cors',
            // 'Sec-Fetch-Dest': 'empty',
            // 'Referer': 'https://www.uniuni.com/',
            // 'Accept-Encoding': 'gzip, deflate, br, zstd',
            // 'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6'
        };
        
        // 发送GET请求
        const response = await axios.get(url, { headers });
        
        // 返回响应数据
        res.json({
            success: true,
            data: response.data
        });
        
    } catch (error) {
        logger.error(`获取物流信息失败: ${error.message}`);
        
        // 处理不同类型的错误
        if (error.response) {
            // 服务器返回了错误状态码
            res.status(error.response.status).json({
                success: false,
                error: '请求失败',
                details: error.response.data
            });
        } else if (error.request) {
            // 请求已发送，但没有收到响应
            res.status(500).json({
                success: false,
                error: '没有收到服务器响应',
                details: error.message
            });
        } else {
            // 发生了其他错误
            res.status(500).json({
                success: false,
                error: '请求处理失败',
                details: error.message
            });
        }
    }
});
// 获取FedEx物流信息的接口
app.post('/get_fedex', async (req, res) => {
  try {
    const trackingNumber = req.body.trackingNumber || "61290374823521026095";
    
    const url = "https://api.fedex.com.cn/track/v2/shipments";
    const headers = {
      "Host": "api.fedex.com.cn",
      "Connection": "keep-alive",
      "sec-ch-ua-platform": "Windows",
      "X-version": "1.0.0",
      "sec-ch-ua": "\"Chromium\";v=\"136\", \"Microsoft Edge\";v=\"136\", \"Not.A/Brand\";v=\"99\"",
      "X-clientid": "WTRK",
      "sec-ch-ua-mobile": "?0",
      "X-Requested-With": "XMLHttpRequest",
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
      "Accept": "application/json",
      "Content-Type": "application/json",
      "X-locale": "en_SG",
      "Origin": "null",
      "Sec-Fetch-Site": "same-site",
      "Sec-Fetch-Mode": "cors",
      "Sec-Fetch-Dest": "empty",
      "Referer": `https://www.fedex.com.cn/wtrk/track/?trknbr=${trackingNumber}`,
      "Accept-Encoding": "gzip, deflate, br, zstd",
      "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
      "Cookie": "fdx_cbid=191081130617470295933373631790151; optimizelyEndUserId=oeu1747029595684r0.09409295948546226; gdl-clientId=15f6aa2c-2982-4bc0-855f-8035b8ccd039; _evga_9921={%22uuid%22:%22063d5d1a82a45a8b%22}; _gcl_au=1.1.676979828.1747029606; _cls_v=bab83ccc-d920-4dc1-8dbc-5bad1356dd94; optimizelySession=0; _svtri=40d87de3-0d7c-4a55-af0f-5c0482d69968; QSI_SI_5BHPCesGXcSwPAO_intercept=true; _evga_308d={%22uuid%22:%22063d5d1a82a45a8b%22}; fdx_geo_select=sg; fdx_locale=en_SG; fdx_redirect=en-sg; cc_path=sg; _sfid_1d35={%22anonymousId%22:%22063d5d1a82a45a8b%22%2C%22consents%22:[]}; siteDC=las; Rbt=f0; fdx_bman=10257a816d9970eac2d9cb4347bb69aa; isTablet=false; isMobile=false; isWireless=false; aemserver=PROD-P-dotcom; AMCVS_1E22171B520E93BF0A490D44%40AdobeOrg=1; s_cc=true; g_sref=(direct); at_check=true; _svs=%7B%22c%22%3A%7B%221%22%3Atrue%2C%222%22%3Atrue%2C%223%22%3Atrue%7D%2C%22ct%22%3A1750320111499%2C%22p%22%3A%7B%2243%22%3A1750841555347%2C%222004%22%3A1750841555345%7D%7D; gdl_r42_store_mappings_s={%2243%22:%22trackId#40d87de3-0d7c-4a55-af0f-5c0482d69968#|sessionTs#1750841554824#%22%2C%222004%22:%22marketingCloudVisitorId#51601869476395093923969934266065634199#|tntId#a4d6040a6cfe402ca77a43d8b4edcd57.32_0#|sessionId#9bc2465dd9324de19fffcc9644cb5759#|sessionTs#1750841554824#|mboxEdge#32#%22}; _cls_s=6d9657bf-523b-4b76-b964-f58078f8f5f3:1; QSI_HistorySession=https%3A%2F%2Fwww.fedex.com.cn%2Ffedextrack%2F%3Ftrknbr%3D61290374823521046482%26trkqual%3D20250619034600~61290374823521046482~FXSP~1750834063533%7Chttps%3A%2F%2Fwww.fedex.com.cn%2Fen-sg%2Fhome.html~1750841587201; xacc=CN; s_invisit=true; g_stime=1750924195062; s_vnum=1750953599999&vn=1; ADRUM=s=1750924198908&r=https%3A%2F%2Fwww.fedex.com.cn%2Ffedextrack%2F%3Fhash%3D310204406; bm_mi=7977E1C44C9EF6334BF2EB1C2A913F74~YAAQZ/axdTlXDXmXAQAAfgQ3qxx2C+VKeft5OTHqWBSRZcUnSrcuSUwaam+gE/HFdJD4OMa76wGlitdJj18BWkaOcqLEVdA76GLmiRTeqKv+oQS+bZQSs7rqQuUbdZWbSpnUCZIIlpnK94ZiYzTqgpGKp9bMkN5Z2ji6vHU+jb0gACqI2GWwBvdV3mM+FwCySycHxtXfRM0xSw8uR5CUDDcK0XQSaAEYIU5JhRWel7qDpKj8OpU58IW2/OCbpZyKVO+LslAMgMIXbEBOxyNHuDfyXCSyfZOQP9TxD+s2NcwN3ArDaPbP/avKOAA76LhyaC3OvoEvHKzD0zel~1; bm_sv=D56BAB9445624ED8FB70DFC06F7A5CCC~YAAQZ/axdTpXDXmXAQAAfgQ3qxy66hnNEsXweU6IuANe4cV6cLdfF6YwmjowLRLV1kpaXazyHKvCSM8/Pmf3KCX7vEB+zjXR5HpxbDCX7xLxiBNHn6bIKEElmoSyxhEgqIQGRhjoZPGL6cglWDiB72HvTxmbldE/bsCtOLgok4tp+YTb90oy2gfEin6BuNZqKkXxuXMAoXOgtM6JJ9dlT0Y1WF1U/PfWCDR2JUCQrLReXeUpX5mGmMFlPX0NZiIgBnQ=~1; bm_sz=B5A6E6AFEF86FD8869566549BB796C08~YAAQZ/axdTtXDXmXAQAAfgQ3qxyjYK/qNFZhiRo/mMVTBrCm33iDvCSbXfqc199ebDSc3Nyj5Gg2Lc48Hintb9fyZ48X2BWeSU4X7cNAmJr5XrmivVvZQXw+2r2dY+AqbcyIAgr5pdyMQKbV6yw1Gha7XursZMi3B7i5aNtOvvoIsVpaduNsL2TVe5daJGBctheWW2uULisqrUwNAeinJtZvt4Cy6i4Xlb/HsRAAHdcMFu8gdJiPk7BrwQeqGPMrEEx9PWzGE2UWoBzE5zCcIpWxgIUuGeoXFDynDNddrjNSd3nN0yl6RaV0FssyPA0Wq/wqeSq1Q1v6vKQYfHoqw9k9ZpNd62r77Jlb4qAAFnN78/Tr6UE2+JplbwP9R5RMa2fkpvVB0aH03swGap4nXmiYgpt3x4copZKPVS+DoMGZwMmqOxtCMqNzMzKD5qZcgktoWiJjiw==~3621937~3355193; s_ppv=undefined%2C0%2C0%2C0; mbox=PC#a4d6040a6cfe402ca77a43d8b4edcd57.32_0#1814169001|session#284847a4c2db417ebf0c5b1241561e11#1750926061; mboxEdgeCluster=32; ak_bmsc=F0CB06586D9BD0E12AB32ED074039AC2~000000000000000000000000000000~YAAQZ/axdVBXDXmXAQAA2gk3qxxx9svQa3Gd5EvzLi2W3T4/PShNoqYfWn4FXBQEJMbPFeQ54wdjfcwZV8BmIPJrSe3obxnsF+gJHEr7VfqNgFJcIK6ssfTQi8Y3ZPLfd/bWbKbjsi4TZ26/B89o0SmmMl1ELd6tfNAY4po+aycTH/nfpHaZD2Na2KHjwxiz4hMv2XzBE9pyc+s52Y278SdpY+rza4TevUtIt873pUr6KHnnYwp3xXuBxXe7xWtOaHBZgk/55OTNBft6vLSR/jqMlTQvYPPticNjflE6HjrutKgFH02jserovgbtwhNFEEK4XvJQOpNFsl/nuOB6rD9d/twEtW+DOxL8Tr1RgiTg0ZjafzolIhDx561soU7u8JXqV3xRNsXvIACi3uny5F9kxsVGnNt7JiCSadQSVb4rb/hXet0+3fqLVc+ziIe34unSgtbKkqB5ePUPtcBVNIDp81YObN+eWHi87l8D+C0XHBEW64uN; _abck=1C43AB07B24A803A6FAE40340D8A14F5~0~YAAQbfaxdel5dXuXAQAAkAs3qw4eSkvvQDDoVe9ebvO61RiZCKwD9mbSzTqv7ILlqG0mxlwvfsQEn6ElAkhIOea9YqnghWSJ7VfhMldXmNStAusJSee3FkMQn0uRWqgV7QrJLQJq9MvyKs8ejyqe958ookO+tNq0ShnukXoLBz9wSxcH2jnwaiWRrssyoCs8Ynpqbj76xRw8jWPyyZFiUn5c9o2xYAdvOMVIkdnT67tZvCStnHzAHJnLc7xfizsFh54vEMYgLR9vUjYbQfqrM1NoyQybPMOkklCz9yBufyzHfTbLb4Fjyf5LRh0PItcPRShHNna/yyjpwE4qQtIij8SoP0Cmx7DWTcq2q+MOup+Npuw3p8AhtThuBMo83t6+fp0axlD1v45C0hbsAAF+xmgxgglX1rvo4nDy/qpJrJBXRFaaKKCgz3A7xIZzTOwqZeDLDtOi6BnvitdYZ08S1XHZLWDKHjVVHzCg5XeifyF1v043VTX9cbPVuyioGLXh2GaSZ4bY6+TsBhbOgrks+z60DCcjLobf8+fkqMqqCeZ5AdI8RWM0g32Z9LA0WCvEBoI43cRfa4BdIsCVu4/CsWud2e+S/uhTuR28NaVlD7JccfPNuyOddJmPe1JjGO8SMR2XjsK68nTdmKOxzhBfWtp8jrdkr/Efr/tSlmgdXcfqHvPEkTuk1FHXiKqvT7bkFeolPNUiEoPMshP3aBpbyv2EC2F4Zqkm8lwTasMWlq4ySjeXdXrGh3Dx0OmdbQAEFBWCXJ4DlilKe+gk/CVBa/U+38Tio3IKszNFp5c5cRVRPxRNL3RgdpAqFeOLni1XacnsRvVsszRCn+Ft7EO3rAnwTu6ysrZsogV+kHJEQz6GHZ6jR4GzmzvLPOcIs+EP~-1~-1~-1; gpv_pageName=fedex/home; AMCV_1E22171B520E93BF0A490D44%40AdobeOrg=359503849%7CMCIDTS%7C20265%7CMCMID%7C51601869476395093923969934266065634199%7CMCAAMLH-1751529004%7C11%7CMCAAMB-1751529004%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1750931404s%7CNONE%7CMCAID%7CNONE%7CMCSYNCSOP%7C411-20228%7CvVersion%7C5.0.1"
    };
    
    const requestBody = {
      "appDeviceType": "WTRK",
      "appType": "WTRK",
      "supportHTML": true,
      "supportCurrentLocation": true,
      "trackingInfo": [
        {
          "trackNumberInfo": {
            "trackingCarrier": "",
            "trackingNumber": trackingNumber
          }
        }
      ],
      "uniqueKey": "",
      "guestAuthenticationToken": ""
    };
    
    const response = await axios.post(url, requestBody, { headers, timeout: 30000 });
    
    res.json({
      success: true,
      data: response.data,
      trackingNumber: trackingNumber
    });
    
  } catch (error) {
    logger.error(`获取FedEx物流信息失败: ${error.message}`);
    
    if (error.response) {
      res.status(error.response.status).json({
        success: false,
        error: '请求失败',
        details: error.response.data,
        trackingNumber: req.body.trackingNumber
      });
    } else {
      res.status(500).json({
        success: false,
        error: '内部服务器错误',
        details: error.message,
        trackingNumber: req.body.trackingNumber
      });
    }
  }
});
// 新增接口：获取物流信息
app.post('/get_wuliu', async (req, res) => {
    try {
        // 从请求体中获取追踪号码数组（如果有多个单号可传入，默认示例为单个单号）
        const { trackingNumbers = ["1ZK6B4420339000466"] } = req.body;
        
        // 检查追踪号码是否存在
        if (!trackingNumbers || trackingNumbers.length === 0) {
            return res.status(400).json({
                success: false,
                error: '缺少必要参数: trackingNumbers'
            });
        }
        
        // 构建请求URL
        const url = "http://us-api.youngcargo.cn/order/track";
        
        // 设置请求头
        const headers = {
            'Content-Type': 'application/json;charset=UTF-8',
            'token': 'YoungCargo.token::eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjAzMzIiLCJpYXQiOjE3NTE4NTIxNDIsImxvZ2luLXVzZXItaWQiOjEyMDMzMiwidXNlcm5hbWUiOiJOMDAxIiwibG9naW4tY29tcGFueS1jb2RlIjoiYTlkOTFhZTM4MjhhNGU2ZmFjMGY3NDA1MDhmZWI1NzQiLCJsb2dpbi1jaGFubmVsIjoiQURNSU4ifQ.o_HL24Q5K8RWAbZGyswNjFcLAkAQYHjXzbN0N6tChYg'
        };
        
        // 构建请求体（使用从请求中获取的追踪号码，默认为示例单号）
        const requestBody = trackingNumbers;
        
        // 发送POST请求
        const response = await axios.post(url, requestBody, { headers });
        
        // 返回响应数据
        res.json({
            success: true,
            data: response.data,
            message: "物流信息获取成功"
        });
        
    } catch (error) {
        console.error(`获取物流信息失败: ${error.message}`);
        
        // 处理不同类型的错误
        if (error.response) {
            // 服务器返回了错误状态码
            res.status(error.response.status).json({
                success: false,
                error: '请求失败',
                details: error.response.data,
                message: error.response.statusText
            });
        } else if (error.request) {
            // 请求已发送，但没有收到响应
            res.status(500).json({
                success: false,
                error: '没有收到服务器响应',
                details: error.message,
                message: '请检查网络连接或API地址'
            });
        } else {
            // 发生了其他错误
            res.status(500).json({
                success: false,
                error: '请求处理失败',
                details: error.message,
                message: '请稍后再试'
            });
        }
    }
});
// 新增接口：获取fuhai.json的所有数据
app.get('/get_fuhai', async (req, res) => {
    try {
        // 读取fuhai.json文件
        const data = await fs.promises.readFile('fuhai.json', 'utf8');
        
        // 解析JSON数据
        const jsonData = JSON.parse(data);
        
        // 返回成功响应
        res.json({
            success: true,
            data: jsonData
        });
    } catch (error) {
        logger.error(`获取fuhai.json数据失败: ${error.message}`);
        
        // 处理不同类型的错误
        if (error.code === 'ENOENT') {
            // 文件不存在
            res.status(404).json({
                success: false,
                error: '数据文件不存在'
            });
        } else if (error instanceof SyntaxError) {
            // JSON解析错误
            res.status(500).json({
                success: false,
                error: '数据文件格式错误'
            });
        } else {
            // 其他错误
            res.status(500).json({
                success: false,
                error: '获取数据失败',
                details: error.message
            });
        }
    }
});    
// 新增接口：获取物流相关账户信息并累加写入带日期的数据（保留最近10天）
app.post('/get_fws', async (req, res) => {
    try {
        // 存储所有接口返回的结果
        const result = {};

        // 1. OJ平台
        const url1 = "http://47.119.185.123:833/Home/GetTotalShipment";
        const headers1 = {
            'Host': '47.119.185.123:833',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'http://47.119.185.123:833/Home/homepage2',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Cookie': 'lang=zh-CN; ASP.NET_SessionId=szbolo1pnucn5t1dtejf21gl; UserName=60002; Password=E4529B2BAC32FBCE9750D448280065240FEE5FC0; Hm_lvt_d214947968792b839fd669a4decaaffc=1751622414,1751851888,1751953379,1752119773; HMACCOUNT=37C62D497DFA9153; IceFire_cusloginuserkey_2016=353663BD14F7973076E1738506413CB78C706CBCF24A0D0673E004C8D5FD0B4B55A8B719CA9115719809823A852E30C99BDE5B361A10DC2C7062AD95FB281FE62EEDE062D1E5D3A6BDB5B9D63A7FD21865316CF04BC848CCA2A8C42BFA717D9EB27C67957035C7EB5050938E480EEFD489F982BCEF02C66A8C9A7E43A8CCDBE846C79943E10A0BEE6D8FA9A8BD122903812DD4BFF0366E74656E38A051C272FE304AF8D40434AF980FA0896C645D57CBE3E67D729752F36326084328764D031E238824D994AB9F9D18A04371DB292EDF6148F21BA72E94223E8DF0C4148A08B47AF2A6AD587D617A18797F9BA36CF4FEF477D1073FE399F5B008E416EE885AAF03FB8E375E57DD3976E8504D8CD676C3F2B92E1AF76A144374483297DB4E5B6F893EFB7513BA39AD25CA95B43B45E49D5B029DCDC1AC11F4FE95DA3D93005F6FC79F5DF4A63C5711084388F5AD0F657457E4EC5C1926BF327C5F05A23D77D5DFC1F84994217FA4C308CCC1ACD2E4D59180C8923FE53662416F78C1733F45D7DA0C1854FD346ABDAFB59A8DCC787928313A223362259DB6F49A04880434B8EAB9250BA8EDC313303D8A005B4DFFAE83A6906AB07E1C44DF58BAEBAD9D5B29765A92E8AE8D56F7DF097A8B2BAE1BA9193106E7BF5F1E4FCC027B78116E1833BB7FDF51BB7A30F4F2400C7699BDD1CCE7E20AB25EFACF1685774B1DCFC5A85559566E4F7780ABBD3C0B73CCB15D5960F0D20E56326DDA7D6A6B165E9FC784AEAEDBE3136A95D52C654D96A7C2AC91FE827C3919EA956E23C99E017CB192BBB1B3BE8CFCAAEC92D4823B59963C16593FFE2AB57B35529CFAC050EBA3996EB336BA3D0AD72CB259858564727EE3F7FB2A7EC1073C51A0347E0FC7ABABB4EB88669677; Hm_lpvt_d214947968792b839fd669a4decaaffc=1752125746'
        };
        const response1 = await axios.get(url1, { headers: headers1 });
        
        // 验证第一个平台数据
        if (!response1.data || typeof response1.data.AccountMoney === 'undefined') {
            throw new Error('OJ平台响应格式不符合预期');
        }
        
        // OJ平台：余额=AccountMoney，假设无信用额度
        const ojBalance = Number(response1.data.AccountMoney);
        const ojCredit = 0;
        result.OJ = {
            平台名称: "OJ",
            余额: ojBalance,
            信用额度: ojCredit,
            总额度: ojBalance + ojCredit,
            数据来源: "http://47.119.185.123:833/Home/GetTotalShipment"
        };

        // 2. 山凯平台
        const url2 = "http://www.gogoship.cn/api/oms/users/getCompanyInfo";
        const headers2 = {
            'Host': 'gogoship.cn',
            'Connection': 'keep-alive',
            'Content-Length': '0',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'X-customer-token': '',
            'token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTIyODY0ODIsInN1YiI6ImpPbXMiLCJuYmYiOjE3NTIwMjcyODIsImF1ZCI6IkpPTVMiLCJpYXQiOjE3NTIwMjcyODIsImp0aSI6IjA0OTk0ZTIyNmZiYmI0MjBkNjdhZDU2MDA2ZDY4MDJkIiwiaXNzIjoiak9NUyIsInN0YXR1cyI6MSwiZGF0YSI6eyJ1X2lkIjoxMDUsInVfbmFtZSI6ImxpbnlpYmluQGFveTU2LmNuIiwidV9zeXN0ZW0iOiJTSzI4NTYiLCJ1X2VudGl0eV9sb2dvIjoiIiwicm9sZV9pZCI6IjEwMDAwIiwidV9lbnRpdHlfaWQiOjF9fQ.oCvcc4NbBfMNxRZ3KA9La16-NuvqgUYTjiwU3r1Ym08',
            'Origin': 'http://www.gogoship.cn',
            'Referer': 'http://www.gogoship.cn/',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6'
        };
        const response2 = await axios.post(url2, {}, { headers: headers2 });
        
        // 验证第二个平台数据
        if (!response2.data || !response2.data.result || typeof response2.data.result.company_balance === 'undefined') {
            throw new Error('山凯平台响应格式不符合预期');
        }
        
        // 处理带逗号的金额
        const rawBalance = response2.data.result.company_balance;
        const shankaiBalance = Number(rawBalance.replace(/,/g, ''));
        const shankaiCredit = 0;
        
        result.山凯 = {
            平台名称: "山凯",
            余额: shankaiBalance,
            信用额度: shankaiCredit,
            总额度: shankaiBalance + shankaiCredit,
            货币单位: response2.data.result.currency_code || "USD",
            数据来源: "http://www.gogoship.cn/api/oms/users/getCompanyInfo"
        };

        // 3. 八达通平台
        const url3 = "https://gdybsjs.oms.eccangtms.com/alpaca-oms/oms/omsUserAccount/list?_t=1752126134&lang=";
        const headers3 = {
            'Host': 'gdybsjs.oms.eccangtms.com',
            'Connection': 'keep-alive',
            'X-Access-Token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzeXN0ZW1CZWxvbmciOiIzIiwibGFuZyI6IiIsImV4cCI6MTc1MjE0MDUwNywidXNlcklkIjoiMTg5NTM2NjcxODQ5MzMzNTU1NCJ9.a14bNEtcMFnw-LCkpUHmD3le-3nbfjzgpKvsnHt39WE',
            'sec-ch-ua-platform': '"Windows"',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
            'Accept': 'application/json, text/plain, */*',
            'X-System-Belong': '3',
            'sec-ch-ua': '"Chromium";v="136", "Microsoft Edge";v="136", "Not.A/Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': 'https://gdybsjs.oms.eccangtms.com/financial/overview',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6'
        };
        const response3 = await axios.get(url3, { headers: headers3 });
        
        // 验证第三个平台数据
        if (!response3.data || !response3.data.result || !Array.isArray(response3.data.result)) {
            throw new Error('八达通平台响应格式不符合预期');
        }
        
        // 提取八达通USD余额和信用额度
        const bdatongAccount = response3.data.result.find(item => item.currencyCode === 'USD');
        if (!bdatongAccount) {
            throw new Error('八达通平台未找到USD账户信息');
        }
        const bdatongBalance = Number(bdatongAccount.accountBalance);
        const bdatongCredit = Number(bdatongAccount.creditAccount);
        result.八达通 = {
            平台名称: "八达通",
            余额: bdatongBalance,
            信用额度: bdatongCredit,
            总额度: bdatongBalance + bdatongCredit,
            数据来源: "https://gdybsjs.oms.eccangtms.com/alpaca-oms/oms/omsUserAccount/list"
        };

        // 4. YC平台
        const url4 = "http://us-api.youngcargo.cn/wallet-recharge/getFirstPageCount?currency=USD";
        const headers4 = {
            'token': 'YoungCargo.token::eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjAzMzIiLCJpYXQiOjE3NTIxMjY0MjksImxvZ2luLXVzZXItaWQiOjEyMDMzMiwidXNlcm5hbWUiOiJOMDAxIiwibG9naW4tY29tcGFueS1jb2RlIjoiYTlkOTFhZTM4MjhhNGU2ZmFjMGY3NDA1MDhmZWI1NzQiLCJsb2dpbi1jaGFubmVsIjoiQURNSU4ifQ.x5knZfggV2rl-T1shbZzQt9azd4oip8j6f3X4MMXSYA'
        };
        const response4 = await axios.get(url4, { headers: headers4 });
        
        // 验证第四个平台数据
        if (!response4.data || !response4.data.data) {
            throw new Error('YC平台响应格式不符合预期');
        }
        
        // YC平台：余额=wallet，信用额度=credit
        const ycBalance = Number(response4.data.data.wallet);
        const ycCredit = Number(response4.data.data.credit);
        result.YC = {
            平台名称: "YC",
            余额: ycBalance,
            信用额度: ycCredit,
            总额度: ycBalance + ycCredit,
            数据来源: "http://us-api.youngcargo.cn/wallet-recharge/getFirstPageCount"
        };

        // 5. 衍溢平台
        const url5 = "https://oms.xlwms.com/gateway/woms/account/detail";
        const headers5 = {
            'Host': 'oms.xlwms.com',
            'Connection': 'keep-alive',
            'sec-ch-ua-platform': '"Windows"',
            'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIlN0IlMjJidXNpbmVzc1R5cGUlMjIlM0ElMjJvbXMlMjIlMkMlMjJsb2dpbkFjY291bnQlMjIlM0ElMjJGT09IQVklMjIlMkMlMjJ1c2VyTmFtZUNuJTIyJTNBJTIyJTIyJTJDJTIydXNlck5hbWVFbiUyMiUzQSUyMiUyMiUyQyUyMmN1c3RvbWVyQ29kZSUyMiUzQSUyMjEzOTgxMDYlMjIlMkMlMjJ0ZW5hbnRDb2RlJTIyJTNBbnVsbCUyQyUyMnRlcm1pbmFsVHlwZSUyMiUzQW51bGwlN0QiLCJpc3MiOiJ4aW5nbGlhbi5zZWN1cml0eSIsImJ1c2luZXNzVHlwZSI6Im9tcyIsImV4cCI6MTc1MjE5ODA4NSwiaWF0IjoxNzUyMTExNjg1LCJqdGkiOiI4YjY4ZDE2My0wNDI5LTRkNWUtYjllZS02ZGIyNzEwYzlhMWEifQ.DRS83MqXJOExxpjMnQ8XxDGe68JfMWPxFCigfA0je_M',
            'lang': 'zh',
            'sec-ch-ua': '"Chromium";v="136", "Microsoft Edge";v="136", "Not.A/Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
            'Accept': 'application/json, text/plain, */*',
            'version': 'prod',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': 'https://oms.xlwms.com/account/my/list',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Cookie': '_hjSessionUser_3119560=eyJpZCI6ImNiZWViYmY4LTk4ZTctNTM3Yy1iYmFhLWJhODc0NTRhYWE2MSIsImNyZWF0ZWQiOjE3NDY2NzAxOTM4MDcsImV4aXN0aW5nIjp0cnVlfQ==; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22196ada80c1d991-0bcb33c3099f38-4c657b58-2073600-196ada80c1e213b%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTk2YWRhODBjMWQ5OTEtMGJjYjMzYzMwOTlmMzgtNGM2NTdiNTgtMjA3MzYwMC0xOTZhZGE4MGMxZTIxM2IifQ%3D%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%7D; _gid=GA1.2.717587849.1751851399; version=prod; _ga_NRLS16EKKE=GS2.1.s1752118341$o250$g1$t1752118386$j15$l0$h0; _ga=GA1.2.1469607317.1746670190; _hjSession_3119560=eyJpZCI6ImVmMzk3MTQyLTU3NGUtNDE3Ny04MTAzLTI5MmE1MzhkYzJjYyIsImMiOjE3NTIxMjY3NTU2MzcsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; prod=always; _ga_2HTV43T3DN=GS2.1.s1752126756$o257$g1$t1752126766$j50$l0$h0'
        };
        const response5 = await axios.get(url5, { headers: headers5 });
        
        // 验证第五个平台的数据
        if (!response5.data || !response5.data.data || !Array.isArray(response5.data.data)) {
            throw new Error('衍溢平台响应格式不符合预期');
        }
        
        // 提取衍溢USD对应的余额和信用额度
        const yanyiAccount = response5.data.data.find(item => item.currencyCode === 'USD');
        if (!yanyiAccount) {
            throw new Error('衍溢平台未找到USD账户信息');
        }
        const yanyiBalance = Number(yanyiAccount.holdValue);
        const yanyiCredit = Number(yanyiAccount.creditValue);
        result.衍溢 = {
            平台名称: "衍溢",
            余额: yanyiBalance,
            信用额度: yanyiCredit,
            总额度: yanyiBalance + yanyiCredit,
            数据来源: "https://oms.xlwms.com/gateway/woms/account/detail"
        };

        // 计算所有平台的总余额、总信用额度、总总额度
        const allPlatforms = Object.values(result);
        const totalBalance = allPlatforms.reduce((sum, item) => sum + item.余额, 0);
        const totalCredit = allPlatforms.reduce((sum, item) => sum + item.信用额度, 0);
        const totalAll = allPlatforms.reduce((sum, item) => sum + item.总额度, 0);

        // ********** 保存数据到 fws.json（保留最近10天）**********
        const fs = require('fs');
        const path = require('path');
        
        // 获取当前日期（格式：YYYY-MM-DD）
        const now = new Date();
        const dateStr = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
        
        // 格式化每个平台的数据（添加日期前缀）
        const formattedData = allPlatforms.map(platform => {
            return `日期:${dateStr},平台名称:"${platform.平台名称}","余额":${platform.余额.toFixed(3)},"信用额度":${platform.信用额度.toFixed(3)},"总额度":${platform.总额度.toFixed(3)}`;
        }).join('\n');
        
        // 定义文件路径
        const filePath = path.join(__dirname, 'fws.json');
        
        // 读取现有文件内容（如果文件不存在则创建空内容）
        let existingContent = '';
        if (fs.existsSync(filePath)) {
            existingContent = fs.readFileSync(filePath, 'utf8');
        }
        
        // 解析现有数据，提取所有日期
        const lines = existingContent.split('\n').filter(line => line.trim() !== '');
        const dates = [...new Set(lines.map(line => line.split(',')[0].replace('日期:', '')))];
        
        // 如果日期数量超过10天，删除最早的日期数据
        if (dates.length >= 10) {
            // 按日期排序
            dates.sort((a, b) => new Date(a) - new Date(b));
            const oldestDate = dates[0];
            
            // 过滤掉最早日期的数据
            const filteredLines = lines.filter(line => !line.startsWith(`日期:${oldestDate}`));
            existingContent = filteredLines.join('\n');
            
            // 如果过滤后有内容，确保最后有两个空行（用于分隔新数据）
            if (existingContent) {
                existingContent += '\n\n';
            }
        } else if (existingContent) {
            // 如果不足10天且有现有内容，添加两个空行分隔新数据
            existingContent += '\n\n';
        }
        
        // 累加新数据
        const contentToWrite = existingContent + formattedData;
        
        // 写入文件
        fs.writeFileSync(filePath, contentToWrite, 'utf8');
        console.log(`数据已成功保存到 ${filePath}（保留最近10天）`);

        // 返回接口响应
        res.json({
            success: true,
            data: {
                platforms: result,
                summary: {
                    总余额: totalBalance,
                    总信用额度: totalCredit,
                    总总额度: totalAll
                }
            },
            message: "物流相关账户信息获取成功并已保存到fws.json"
        });

    } catch (error) {
        console.error(`获取物流相关账户信息失败: ${error.message}`);
        
        // 错误处理
        if (error.response) {
            res.status(error.response.status).json({
                success: false,
                error: '请求失败',
                details: error.response.data,
                message: error.response.statusText
            });
        } else if (error.request) {
            res.status(500).json({
                success: false,
                error: '没有收到服务器响应',
                details: error.message,
                message: '请检查网络连接或API地址'
            });
        } else {
            res.status(500).json({
                success: false,
                error: '数据处理失败',
                details: error.message,
                message: '请检查API响应格式是否变化'
            });
        }
    }
});
// 新增接口：将数据写入fuhai.json
app.post('/post_fuhai', async (req, res) => {
    try {
        // 获取请求体中的数据
        const data = req.body;
        
        // 验证数据是否存在
        if (!data || Object.keys(data).length === 0) {
            return res.status(400).json({
                success: false,
                error: '缺少数据'
            });
        }
        
        // 将数据写入fuhai.json文件
        await fs.promises.writeFile('fuhai.json', JSON.stringify(data, null, 2), 'utf8');
        
        // 返回成功响应
        res.json({
            success: true,
            message: '数据已成功写入fuhai.json'
        });
    } catch (error) {
        logger.error(`写入fuhai.json数据失败: ${error.message}`);
        
        // 处理不同类型的错误
        res.status(500).json({
            success: false,
            error: '写入数据失败',
            details: error.message
        });
    }
});    
// 物流数据查询接口（默认近7天，支持多参数筛选，仓库模糊匹配）
app.get('/kh_wuliu', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        // 修复：在解构中添加outboundNumber参数
        const {
            customerName, trackingNumber, channel, channelGroup, warehouse,
            logisticsProvider, logisticsStatus, startDate, endDate,
            deliveryStart, deliveryEnd, onlineStart, onlineEnd,
            outboundNumber  // 新增：添加出库单号参数的解构声明
        } = req.query;

        const conditions = [];
        const values = [];
        
        // 检查是否有任何查询参数
        const hasAnyParameter = Object.keys(req.query).length > 0;
        
        // 时间条件处理函数
        const processDateRange = (start, end, fieldName) => {
            if (start || end) {
                // 处理开始日期
                let processedStart = start;
                if (processedStart && !processedStart.includes(' ')) {
                    processedStart = `${processedStart} 00:00:00`;
                }
                
                // 处理结束日期
                let processedEnd = end;
                if (processedEnd && !processedEnd.includes(' ')) {
                    processedEnd = `${processedEnd} 23:59:59`;
                }
                
                console.log(`时间字段 ${fieldName} 原始值:`, { start, end });
                console.log(`时间字段 ${fieldName} 处理后:`, {
                    processedStart,
                    processedEnd
                });
                
                if (processedStart && processedEnd) {
                    conditions.push(`${fieldName} BETWEEN ? AND ?`);
                    values.push(processedStart, processedEnd);
                } else if (processedStart) {
                    conditions.push(`${fieldName} >= ?`);
                    values.push(processedStart);
                } else if (processedEnd) {
                    conditions.push(`${fieldName} <= ?`);
                    values.push(processedEnd);
                }
                
                return true; // 表示该时间字段已被处理
            }
            
            return false; // 表示该时间字段未被处理
        };

        // 处理各时间字段
        processDateRange(startDate, endDate, '出库时间');
        processDateRange(onlineStart, onlineEnd, '上网时间');
        processDateRange(deliveryStart, deliveryEnd, '妥投时间');

        // 关键逻辑：只有当没有任何查询参数时，才添加默认的近7天条件
        if (!hasAnyParameter) {
            conditions.push(`出库时间 >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)`);
            console.log('添加默认时间条件: 出库时间 >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)');
        }

        // 其他筛选条件
        if (customerName) {
            conditions.push('客户名称 = ?');
            values.push(customerName);
        }
        if (trackingNumber) {
            conditions.push('跟踪单号 = ?');
            values.push(trackingNumber);
        }
        // 出库单号筛选条件
        if (outboundNumber) {
            conditions.push('出库单号 = ?');
            values.push(outboundNumber);
        }
        if (channel) {
            conditions.push('渠道 = ?');
            values.push(channel);
        }
        if (warehouse) {
            conditions.push('仓库 LIKE ?');
            values.push(`%${warehouse}%`);
        }
        if (logisticsProvider) {
            conditions.push('物流商 = ?');
            values.push(logisticsProvider);
        }
        if (logisticsStatus) {
            conditions.push('物流状态 = ?');
            values.push(logisticsStatus);
        }

        // 构建查询 - 包含出库单号字段
        const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
        const query = `
            SELECT 
                ID, 客户名称, 跟踪单号, 渠道, 渠道组, 出库时间, 上网时间, 妥投时间,
                最新轨迹详情, 最新轨迹时间, 上网时效, 运输时效, 物流商, 物流状态, 仓库,
                出库单号  
            FROM wuliu_data
            ${whereClause}
            ORDER BY 出库时间 DESC
        `;

        // 输出完整的调试信息
        console.log('执行SQL:', query);
        console.log('查询参数:', values);
        
        const [rows] = await conn.query(query, values);

        console.log(`查询结果: ${rows.length} 条记录`);
        res.json({
            success: true,
            data: rows,
            message: `共查询到 ${rows.length} 条数据`
        });

    } catch (error) {
        console.error(`物流数据查询失败: ${error.message}`);
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    } finally {
        conn.release();
    }
});



// 新增jianhuo_data接口 - 自动写入数据（避免重复写入）
app.get('/jianhuo_data', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        // 计算出库日期：当前日期 + 1天
        const tomorrow = new Date();
        tomorrow.setDate(tomorrow.getDate() + 1);
        // 格式化日期为YYYY-MM-DD
        const outDate = tomorrow.toISOString().split('T')[0];
        
        // 步骤1：查询该出库日期是否已存在数据
        const [existingData] = await conn.query(
            'SELECT 1 FROM jianhuo_data WHERE 出库日期 = ? LIMIT 1',
            [outDate]
        );
        
        // 若已存在数据，直接返回提示，不执行插入
        if (existingData.length > 0) {
            return res.json({
                success: true,
                message: `出库日期为${outDate}的数据已存在，无需重复写入`,
                outDate,
                count: 0
            });
        }
        
        // 步骤2：若不存在，准备插入数据
        // 定义拣货小时范围：23:00-6:00（共8个小时）
        const hours = [23, 0, 1, 2, 3, 4, 5, 6];
        
        const values = hours.map(hour => [
            outDate,
            hour,
            0, // 拣货单量，默认0
            0  // 当日应出库单量，默认0
        ]);
        
        // 执行批量插入
        const [result] = await conn.query(
            'INSERT INTO jianhuo_data (出库日期, 拣货小时, 拣货单量, 当日应出库单量) VALUES ?',
            [values]
        );
        
        logger.info(`成功写入${result.affectedRows}条拣货数据，出库日期：${outDate}`);
        
        res.json({
            success: true,
            message: `成功写入${result.affectedRows}条数据`,
            outDate,
            count: result.affectedRows
        });
    } catch (error) {
        logger.error(`写入拣货数据出错: ${error.stack}`);
        res.status(500).json({
            success: false,
            error: '服务器内部错误，写入数据失败',
            details: error.message
        });
    } finally {
        conn.release();
    }
});

// 新增获取客户信息并写入数据库的接口
app.get('/customer_info', async (req, res) => {
    // 从连接池获取连接（与jianhuo_data接口保持一致）
    const conn = await pool.getConnection();
    try {
        const baseUrl = 'https://omp.xlwms.com/gateway/omp/customer/list';
        const size = 200; // 每页200条数据
        const type = 1;
        let currentPage = 1;
        let allRecords = [];
        let totalPages = 1;

        // 首次请求获取总页数
        const firstUrl = `${baseUrl}?current=${currentPage}&size=${size}&type=${type}`;
        const firstResponse = await instance.get(firstUrl, { headers: config.externalApi.headers });

        // 校验接口返回状态
        if (firstResponse.data.code !== 200) {
            throw new Error(`接口返回错误：${firstResponse.data.msg || '首次请求失败'}`);
        }

        const firstData = firstResponse.data.data;
        allRecords = allRecords.concat(firstData.records); // 合并第一页数据
        totalPages = firstData.pages; // 总页数

        // 分页请求剩余数据（若总页数>1）
        if (totalPages > 1) {
            for (let page = 2; page <= totalPages; page++) {
                const url = `${baseUrl}?current=${page}&size=${size}&type=${type}`;
                const response = await instance.get(url, { headers: config.externalApi.headers });

                if (response.data.code !== 200) {
                    throw new Error(`第${page}页请求失败：${response.data.msg}`);
                }

                allRecords = allRecords.concat(response.data.data.records);
                console.log(`已同步第${page}/${totalPages}页数据`);
            }
        }

        console.log(`所有客户数据获取完成，共${allRecords.length}条`);

        // 格式化数据（映射所需字段）
        const formattedData = allRecords.map(record => ({
            customer_code: record.customerCode,
            customer_name: record.customerName || '',
            admin_account: record.adminAccount || '',
            company_name: record.companyName || '',
            contacter: record.contacter || '',
            address: record.address || '',
            sales_people_name: record.salesPeopleName || '',
            customer_people_name: record.customerPeopleName || '',
            create_time: record.createTime ? new Date(record.createTime) : null,
            update_time: record.updateTime ? new Date(record.updateTime) : null,
            email: record.email || '',
            telephone_no: record.telephoneNo || '',
            warehouse: record.warehouse || ''
        }));

        // 批量插入或更新（基于customer_code唯一键）
        const sql = `
            INSERT INTO customer_info (
                customer_code, customer_name, admin_account, company_name, contacter,
                address, sales_people_name, customer_people_name, create_time, update_time,
                email, telephone_no, warehouse
            ) VALUES ?
            ON DUPLICATE KEY UPDATE
                customer_name = VALUES(customer_name),
                admin_account = VALUES(admin_account),
                company_name = VALUES(company_name),
                contacter = VALUES(contacter),
                address = VALUES(address),
                sales_people_name = VALUES(sales_people_name),
                customer_people_name = VALUES(customer_people_name),
                create_time = VALUES(create_time),
                update_time = VALUES(update_time),
                email = VALUES(email),
                telephone_no = VALUES(telephone_no),
                warehouse = VALUES(warehouse)
        `;

        // 转换为二维数组格式（批量插入要求）
        const values = formattedData.map(item => [
            item.customer_code,
            item.customer_name,
            item.admin_account,
            item.company_name,
            item.contacter,
            item.address,
            item.sales_people_name,
            item.customer_people_name,
            item.create_time,
            item.update_time,
            item.email,
            item.telephone_no,
            item.warehouse
        ]);

        // 使用连接池的conn执行SQL（与jianhuo_data接口保持一致）
        const [result] = await conn.query(sql, [values]);

        res.json({
            success: true,
            message: `数据同步成功`,
            stats: {
                total: allRecords.length,
                inserted: result.affectedRows - result.changedRows, // 新增数量
                updated: result.changedRows, // 更新数量
                pages: totalPages
            }
        });
    } catch (error) {
        // 错误处理（沿用原有日志记录方式）
        if (error.response) {
            logger.error(`接口请求失败：状态码${error.response.status}，响应：${JSON.stringify(error.response.data)}`);
        } else if (error.request) {
            logger.error('接口请求无响应');
        } else {
            logger.error(`数据处理错误：${error.message}`);
        }
        res.status(500).json({
            success: false,
            error: '服务端错误',
            details: error.message
        });
    } finally {
        // 释放连接回连接池（与jianhuo_data接口保持一致）
        conn.release();
    }
});
// 优化后的 jianhuo_dataa 接口（提取待复核数）
app.get('/jianhuo_dataa', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        // 1. 查询符合条件的客户代码（排除DK开头、XXS开头和包含test的客户）
        const [customers] = await conn.query(
            `SELECT customer_code 
             FROM customer_info 
             WHERE customer_name NOT LIKE 'DK%' 
               AND customer_name NOT LIKE 'XXS%'
               AND LOWER(customer_name) NOT LIKE '%test%'
             ORDER BY customer_code`
        );
        const customerCodes = customers.map(c => c.customer_code).join(',');
        if (!customerCodes) {
            throw new Error('未查询到符合条件的客户代码');
        }

        // 2. 动态计算 startTime 和 endTime
        const now = new Date();
        const currentDate = new Date(now);
        
        // startTime：当前日期 - 2天 01:00:00
        const startTimeDate = new Date(currentDate);
        startTimeDate.setDate(startTimeDate.getDate() - 2);
        const startTime = `${startTimeDate.toISOString().split('T')[0]} 01:00:00`;
        
        // endTime：startTime + 2天 01:00:00
        const endTimeDate = new Date(startTimeDate);
        endTimeDate.setDate(endTimeDate.getDate() + 2);
        const endTime = `${endTimeDate.toISOString().split('T')[0]} 01:00:00`;

        // 3. 构造POST请求数据
        const requestData = {
            "current": 1,
            "size": 20,
            "status": "20",
            "customerCodes": customerCodes,
            "logisticsChannel": "",
            "logisticsCarrier": "",
            "orderSourceList": [],
            "expressFlag": "",
            "varietyType": "",
            "salesPlatform": "",
            "timeType": "createTime",
            "orderNoType": "sourceNo",
            "codeType": "barcode",
            "receiver": "",
            "orderCount": "",
            "countKind": "orderWeight",
            "unitMark": 0,
            "relatedReturnOrder": "",
            "appendixFlag": "",
            "forecastStatus": "",
            "countryRegionCodes": "",
            "categoryIdList": [],
            "receiverValue": "",
            "expressPrintStatus": "",
            "withVas": "",
            "productPackType": "",
            "startTime": startTime,
            "endTime": endTime,
            "weightCountStart": "",
            "weightCountEnd": "",
            "sourceNoLists": [],
            "whCode": "NY01"
        };

        // 4. 发送POST请求并处理响应
        const targetUrl = 'https://omp.xlwms.com/gateway/wms/blDelivery/page';
        const response = await instance.post(targetUrl, requestData, {
            headers: config.externalApi.headers
        });

        // 5. 提取返回结果中的 total 作为待复核数
        const { code, data, msg } = response.data;
        if (code !== 200) {
            throw new Error(`接口返回错误：${msg || '未知错误'}`);
        }
        const 待复核数 = data.total || 0; // 核心：将total值标记为待复核数

        // 6. 返回处理结果
        res.json({
            success: true,
            message: "待复核数查询成功",
            requestParams: {
                customerCodes: customerCodes,
                startTime: startTime,
                endTime: endTime
            },
            待复核数: 待复核数, // 明确返回待复核数
            rawResponse: { code, msg, total: data.total } // 可选：保留原始响应中的关键信息
        });

    } catch (error) {
        logger.error(`jianhuo_dataa接口错误: ${error.stack}`);
        res.status(500).json({
            success: false,
            error: "服务器内部错误",
            details: error.message
        });
    } finally {
        conn.release();
    }
});


// 新增 jianhuo_datab 接口：汇总多个状态的total值
app.get('/jianhuo_datab', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        // 1. 查询符合条件的客户代码（排除DK开头、XXS开头和包含test的客户）
        const [customers] = await conn.query(
            `SELECT customer_code 
             FROM customer_info 
             WHERE customer_name NOT LIKE 'DK%' 
               AND customer_name NOT LIKE 'XXS%'
               AND LOWER(customer_name) NOT LIKE '%test%'
             ORDER BY customer_code`
        );
        const customerCodes = customers.map(c => c.customer_code).join(',');
        if (!customerCodes) {
            throw new Error('未查询到符合条件的客户代码');
        }

        // 2. 动态计算 startTime 和 endTime
        const now = new Date();
        const currentDate = new Date(now);
        
        // startTime：当前日期 - 1天 01:00:00
        const startTimeDate = new Date(currentDate);
        startTimeDate.setDate(startTimeDate.getDate() - 1);
        const startTime = `${startTimeDate.toISOString().split('T')[0]} 01:00:00`;
        
        // endTime：startTime + 1天 01:00:00
        const endTimeDate = new Date(startTimeDate);
        endTimeDate.setDate(endTimeDate.getDate() + 1);
        const endTime = `${endTimeDate.toISOString().split('T')[0]} 01:00:00`;

        // 3. 定义需要查询的状态码列表
        const statusList = ["10", "15", "20", "30", "100", "111"];
        // 存储每个状态的total值
        const statusTotals = [];

        // 4. 循环请求每个状态的数据
        for (const status of statusList) {
            // 构造当前状态的请求数据
            const requestData = {
                "current": 1,
                "size": 20,
                "status": status, // 动态传入当前状态码
                "customerCodes": customerCodes,
                "logisticsChannel": "",
                "logisticsCarrier": "",
                "orderSourceList": [],
                "expressFlag": "",
                "varietyType": "",
                "salesPlatform": "",
                "timeType": "createTime",
                "orderNoType": "sourceNo",
                "codeType": "barcode",
                "receiver": "",
                "orderCount": "",
                "countKind": "orderWeight",
                "unitMark": 0,
                "relatedReturnOrder": "",
                "appendixFlag": "",
                "forecastStatus": "",
                "countryRegionCodes": "",
                "categoryIdList": [],
                "receiverValue": "",
                "expressPrintStatus": "",
                "withVas": "",
                "productPackType": "",
                "startTime": startTime,
                "endTime": endTime,
                "weightCountStart": "",
                "weightCountEnd": "",
                "sourceNoLists": [],
                "whCode": "NY01"
            };

            // 发送POST请求
            const targetUrl = 'https://omp.xlwms.com/gateway/wms/blDelivery/page';
            const response = await instance.post(targetUrl, requestData, {
                headers: config.externalApi.headers
            });

            // 校验响应并提取total
            const { code, data, msg } = response.data;
            if (code !== 200) {
                throw new Error(`状态码${status}请求失败：${msg || '未知错误'}`);
            }
            const total = data.total || 0; // 若total为undefined，默认为0
            statusTotals.push({ status, total });
        }

        // 5. 计算所有状态的total之和（今日应出库订单总数）
        const 今日应出库订单总数 = statusTotals.reduce((sum, item) => sum + item.total, 0);

        // 6. 返回结果
        res.json({
            success: true,
            message: "各状态订单数汇总成功",
            requestParams: {
                customerCodes: customerCodes,
                startTime: startTime,
                endTime: endTime,
                statusList: statusList
            },
            各状态订单数: statusTotals, // 明细：每个状态的total
            今日应出库订单总数: 今日应出库订单总数 // 总和
        });

    } catch (error) {
        logger.error(`jianhuo_datab接口错误: ${error.stack}`);
        res.status(500).json({
            success: false,
            error: "服务器内部错误",
            details: error.message
        });
    } finally {
        conn.release();
    }
});

// 获取customer_info表中的客户名称和客服代表
app.get('/customer_info1', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        // 查询客户名称和客服代表，排除DK开头的客户（保持与其他接口一致性）
        const [customers] = await conn.query(
            "SELECT customer_name AS 客户名称, customer_people_name AS 客服代表 " +
            "FROM customer_info " +
            "WHERE customer_name NOT LIKE 'DK%' " +
            "ORDER BY customer_name ASC"
        );

        if (customers.length === 0) {
            return res.json({
                success: true,
                message: "未查询到符合条件的客户数据",
                count: 0,
                data: []
            });
        }

        // 返回查询结果
        res.json({
            success: true,
            message: "客户数据查询成功",
            count: customers.length, // 数据总数
            data: customers // 包含客户名称和客服代表的数组
        });

    } catch (error) {
        logger.error(`customer_info接口错误: ${error.stack}`);
        res.status(500).json({
            success: false,
            error: "服务器内部错误",
            details: error.message
        });
    } finally {
        conn.release();
    }
});
// 服务器端需要增加获取当前时间的接口
app.get('/server_time', async (req, res) => {
    try {
        // 获取当前中国时间(UTC+8)
        const now = new Date();
        const chinaTime = new Date(now.getTime() + (8 * 60 * 60 * 1000));
        
        res.json({
            success: true,
            data: {
                serverTime: chinaTime.toISOString()
            }
        });
    } catch (error) {
        console.error("获取服务器时间接口错误:", error);
        res.status(500).json({
            success: false,
            message: "获取服务器时间失败"
        });
    }
});

// 修复后的评价提交接口
app.post('/pj_submit', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        // 从请求体获取评价数据
        const { 
            customerCode, 
            ratings, 
            feedbackType, 
            detailedFeedback, 
            contactRequested,
            deviceInfo = {}
        } = req.body || {};

        // 验证必填字段
        if (!customerCode || !ratings || ratings.overall === undefined) {
            return res.status(400).json({
                success: false,
                message: "客户代码和总体满意度评分不能为空"
            });
        }

        // 获取当前UTC时间并转换为中国时间(UTC+8)
        const now = new Date();
        const utcTime = new Date(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(),
                                 now.getUTCHours(), now.getUTCMinutes(), now.getUTCSeconds());
        const chinaTime = new Date(utcTime.getTime() + (8 * 60 * 60 * 1000)); // 加上8小时偏移
        
        // 生成评价编号（使用中国时间）
        const year = chinaTime.getFullYear();
        const month = String(chinaTime.getMonth() + 1).padStart(2, '0');
        const day = String(chinaTime.getDate()).padStart(2, '0');
        const hours = String(chinaTime.getHours()).padStart(2, '0');
        const minutes = String(chinaTime.getMinutes()).padStart(2, '0');
        const random = Math.floor(Math.random() * 1000).toString().padStart(3, '0');
        const evaluationId = `FCK${year}${month}${day}${hours}${minutes}${random}`;

        // 评价月份（格式：YYYY-MM）
        const evaluationMonth = `${year}-${month}`;
        
        // 提交时间（中国时间）
        const submitTime = chinaTime;

        // 插入数据库
        const [result] = await conn.query(
            `INSERT INTO customer_evaluation (
                evaluation_id, customer_code, evaluation_month,
                overall_satisfaction, 
                response_speed, problem_solving, professionalism,
                service_attitude, feedback_type, detailed_description,
                need_follow_up, submit_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
                evaluationId,          // 评价编号（前后端将保持一致）
                customerCode,
                evaluationMonth,
                ratings.overall || 0,
                ratings.speed || 0,
                ratings.solution || 0,
                ratings.professionalism || 0,
                ratings.attitude || 0,
                feedbackType || 'other',
                detailedFeedback || '',
                contactRequested ? 1 : 0,
                submitTime  // 存储中国时间
            ]
        );

        // 设备信息处理与存储（可选，不影响主流程）
        try {
            // 提取客户端IP（优先x-forwarded-for）
            const forwardedFor = (req.headers['x-forwarded-for'] || '').toString();
            const clientIp = forwardedFor ? forwardedFor.split(',')[0].trim() : (req.socket && req.socket.remoteAddress) || '';

            // 清洗并截断设备信息，避免过长字符串
            const safeDeviceInfo = {
                user_agent: String(deviceInfo.userAgent || '').slice(0, 512),
                platform: String(deviceInfo.platform || '').slice(0, 64),
                language: String(deviceInfo.language || '').slice(0, 32),
                screen_width: Number(deviceInfo.screen && deviceInfo.screen.width ? deviceInfo.screen.width : 0) || 0,
                screen_height: Number(deviceInfo.screen && deviceInfo.screen.height ? deviceInfo.screen.height : 0) || 0,
                device_memory: deviceInfo.deviceMemory !== undefined && deviceInfo.deviceMemory !== null ? String(deviceInfo.deviceMemory).slice(0, 32) : null,
                hardware_concurrency: Number(deviceInfo.hardwareConcurrency || 0) || null,
                ip: String(clientIp || '').slice(0, 64)
            };

            // 创建设备信息表（如不存在）
            await conn.query(`
                CREATE TABLE IF NOT EXISTS customer_evaluation_device (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    evaluation_id VARCHAR(32) NOT NULL,
                    user_agent VARCHAR(512),
                    platform VARCHAR(64),
                    language VARCHAR(32),
                    screen_width INT,
                    screen_height INT,
                    device_memory VARCHAR(32),
                    hardware_concurrency INT,
                    ip VARCHAR(64),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_eval_id (evaluation_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            `);

            // 插入设备信息
            await conn.query(
                `INSERT INTO customer_evaluation_device (
                    evaluation_id, user_agent, platform, language, screen_width, screen_height, device_memory, hardware_concurrency, ip
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                [
                    evaluationId,
                    safeDeviceInfo.user_agent,
                    safeDeviceInfo.platform,
                    safeDeviceInfo.language,
                    safeDeviceInfo.screen_width,
                    safeDeviceInfo.screen_height,
                    safeDeviceInfo.device_memory,
                    safeDeviceInfo.hardware_concurrency,
                    safeDeviceInfo.ip
                ]
            );
        } catch (deviceErr) {
            // 设备信息写入失败不影响主流程
            console.warn('设备信息保存失败:', deviceErr && deviceErr.message ? deviceErr.message : deviceErr);
        }

        // 返回成功响应（包含中国时间和评价编号）
        res.json({
            success: true,
            message: "评价提交成功",
            data: {
                evaluationId: evaluationId,  // 返回生成的评价编号
                submitTime: submitTime.toISOString()  // 返回中国时间的ISO格式
            }
        });

    } catch (error) {
        console.error("评价提交接口错误:", error);
        res.status(500).json({
            success: false,
            message: "评价提交失败",
            details: error.message
        });
    } finally {
        conn.release();
    }
});
    


// 新增检查客户当月是否已评价的接口
app.post('/feedback_check', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        // 从请求体获取参数
        const { customerCode, evaluationMonth } = req.body;
        
        // 参数验证
        if (!customerCode || !evaluationMonth) {
            return res.status(400).json({
                success: false,
                message: "客户代码和评价月份不能为空"
            });
        }
        
        // 验证月份格式 (YYYY-MM)
        const monthRegex = /^\d{4}-\d{2}$/;
        if (!monthRegex.test(evaluationMonth)) {
            return res.status(400).json({
                success: false,
                message: "评价月份格式不正确，应为YYYY-MM"
            });
        }
        
        // 查询该客户当月是否有评价记录
        const [results] = await conn.query(
            "SELECT COUNT(*) AS count " +
            "FROM customer_evaluation " +
            "WHERE customer_code = ? " +
            "AND evaluation_month = ?",
            [customerCode, evaluationMonth]
        );
        
        // 解析查询结果
        const hasEvaluated = results[0].count > 0;
        
        // 返回响应
        res.json({
            success: true,
            message: hasEvaluated ? "客户当月已评价" : "客户当月未评价",
            data: {
                hasEvaluated: hasEvaluated
            }
        });

    } catch (error) {
        // 错误日志记录，保持与其他接口一致
        logger.error(`feedback_check接口错误: ${error.stack}`);
        res.status(500).json({
            success: false,
            error: "服务器内部错误",
            details: error.message
        });
    } finally {
        // 释放数据库连接
        conn.release();
    }
});

// 新增jianhuo_datac接口 - 正确处理时区转换的日期显示
app.get('/jianhuo_datac', async (req, res) => {
    const conn = await pool.getConnection();
    try {
        const { outDate } = req.query;
        
        if (!outDate) {
            return res.status(400).json({
                success: false,
                error: '参数错误',
                message: '请提供出库日期（outDate）参数，格式为YYYY-MM-DD'
            });
        }
        
        const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
        if (!dateRegex.test(outDate)) {
            return res.status(400).json({
                success: false,
                error: '格式错误',
                message: '出库日期格式不正确，请使用YYYY-MM-DD格式'
            });
        }
        
        // 查询指定出库日期的所有数据
        const [data] = await conn.query(
            'SELECT * FROM jianhuo_data WHERE DATE(出库日期) = ? ORDER BY 拣货小时 ASC',
            [outDate]
        );
        
        // 处理UTC时间转换为本地日期（针对北京时间+8时区）
        const formattedData = data.map(item => {
            // 解析UTC时间字符串
            const utcDate = new Date(item['出库日期']);
            
            // 转换为北京时间（UTC+8）
            const beijingTime = new Date(utcDate.getTime() + 8 * 60 * 60 * 1000);
            
            // 提取年月日部分
            const year = beijingTime.getFullYear();
            const month = String(beijingTime.getMonth() + 1).padStart(2, '0');
            const day = String(beijingTime.getDate()).padStart(2, '0');
            
            return {
                ...item,
                '出库日期': `${year}-${month}-${day}`
            };
        });
        
        res.json({
            success: true,
            message: `成功查询到${formattedData.length}条数据`,
            outDate,
            count: formattedData.length,
            data: formattedData
        });
    } catch (error) {
        logger.error(`查询拣货数据出错: ${error.stack}`);
        res.status(500).json({
            success: false,
            error: '服务器内部错误，查询数据失败',
            details: error.message
        });
    } finally {
        conn.release();
    }
});




// 初始化数据库（创建必要的表）
async function initializeDatabase() {
    const conn = await pool.getConnection();
    try {
        // 创建system_config表（存储仓库日期等配置）
        await conn.query(`
            CREATE TABLE IF NOT EXISTS system_config (
                \`key\` VARCHAR(50) NOT NULL PRIMARY KEY COMMENT '配置键',
                \`value\` VARCHAR(255) NOT NULL COMMENT '配置值',
                \`update_time\` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
            ) COMMENT '系统配置表';
        `);

        // 确保jianhuo_data表存在（如果需要）
        await conn.query(`
            CREATE TABLE IF NOT EXISTS jianhuo_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                出库日期 DATE NOT NULL,
                拣货小时 INT NOT NULL,
                拣货单量 INT NOT NULL DEFAULT 0,
                当日应出库单量 INT NOT NULL DEFAULT 0,
                UNIQUE KEY unique_outdate_hour (出库日期, 拣货小时)
            ) COMMENT '拣货数据记录表';
        `);

        logger.info("数据库初始化完成");
    } catch (error) {
        logger.error(`数据库初始化失败: ${error.message}`);
        throw error;
    } finally {
        conn.release();
    }
}
// 应用启动主流程
async function startApp() {
    try {
        // 1. 初始化数据库表
        await initializeDatabase();
        logger.info('数据库初始化完成');

        // 2. 动态获取customerCodes并更新配置
        await fetchDynamicCustomerCodes();
        logger.info('customerCodes已动态更新');

        // 启动服务器
        app.listen(port, async () => {
            await initializeDatabase();
            logger.info(`服务器已启动，正在监听端口 ${port}`);
        });
    } catch (error) {
        logger.error(`应用启动失败: ${error.stack}`);
        process.exit(1); // 启动失败时退出进程
    }
}

// 执行启动流程
startApp();


// 格式化日期为 YYYY-MM-DD HH:mm:ss 格式
function aaa(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

// 客户余额钉钉推送定时任务
cron.schedule('0 0 9,15 * * *', () => {
    logger.info('开始执行客户余额提醒定时任务');
    sendDingTalkAlert(); // 假设该函数已实现
    fetchDynamicCustomerCodes();
});

// 定时任务 - 每天23:00开始，每小时运行一次，至次日6:00结束
cron.schedule('0 0 23,0-6 * * *', async () => {
    const now = new Date();
    const hour = now.getHours();
    logger.info(`定时任务执行 - 当前时间: ${aaa(now)}, 小时: ${hour}`);
    
    try {
        // 存储所有需要执行的任务
        const tasks = [];
        
        // 23点时执行所有任务（包括初始化）
        if (hour === 23) {
            logger.info('23点执行全量任务: jianhuo_data、customer_info、jianhuo_dataa、jianhuo_datab');
            tasks.push(
                withRetry(() => executeJianhuoData(), 'jianhuo_data'),
                withRetry(() => executeCustomerInfo(), 'customer_info'),
                withRetry(() => executeJianhuoDataa(), 'jianhuo_dataa'),
                withRetry(() => executeJianhuoDatab(), 'jianhuo_datab')
            );
        } else {
            // 非23点时段：仅执行jianhuo_dataa，且仅在1点时额外执行jianhuo_datab
            tasks.push(
                withRetry(() => executeJianhuoDataa(), 'jianhuo_dataa')
            );
            
            // 仅在凌晨1点执行jianhuo_datab
            if (hour === 1) {
                logger.info('凌晨1点执行jianhuo_datab任务');
                tasks.push(
                    withRetry(() => executeJianhuoDatab(), 'jianhuo_datab')
                );
            }
        }
        
        // 并行执行所有任务
        await Promise.all(tasks);
        logger.info('本轮定时任务全部执行完成');
    } catch (error) {
        logger.error(`定时任务执行出错: ${error.message}`, error.stack);
    }
});

// 重试机制包装函数
async function withRetry(task, taskName, maxRetries = 5, retryDelay = 5000) {
    let retries = 0;
    
    while (true) {
        try {
            logger.info(`执行任务: ${taskName}, 第${retries + 1}次尝试`);
            const result = await task();
            logger.info(`任务${taskName}执行成功`);
            return result;
        } catch (error) {
            retries++;
            logger.error(`任务${taskName}执行失败 (第${retries}次重试): ${error.message}`);
            
            if (retries >= maxRetries) {
                logger.error(`任务${taskName}达到最大重试次数(${maxRetries})，停止重试`);
                throw new Error(`任务${taskName}执行失败: ${error.message}`);
            }
            
            // 等待一段时间后重试
            await new Promise(resolve => setTimeout(resolve, retryDelay));
        }
    }
}

// 仓库日期工具函数
async function setWarehouseDate(warehouseDate, conn) {
    try {
        await conn.query(
            'INSERT INTO system_config (`key`, `value`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `value` = ?, update_time = CURRENT_TIMESTAMP',
            ['warehouse_date', warehouseDate, warehouseDate]
        );
        logger.info(`仓库日期已设置为：${warehouseDate}`);
        return warehouseDate;
    } catch (error) {
        logger.error(`设置仓库日期失败：${error.message}`);
        throw error;
    }
}

async function getWarehouseDate(conn) {
    try {
        const [rows] = await conn.query(
            'SELECT `value` FROM system_config WHERE `key` = ? LIMIT 1',
            ['warehouse_date']
        );
        if (rows.length === 0) {
            throw new Error('未找到仓库日期，请确认23点任务已正常执行');
        }
        const warehouseDate = rows[0].value;
        logger.info(`获取到仓库日期：${warehouseDate}`);
        return warehouseDate;
    } catch (error) {
        logger.error(`获取仓库日期失败：${error.message}`);
        throw error;
    }
}

// 各任务执行函数
async function executeJianhuoData() {
    const conn = await pool.getConnection();
    try {
        const tomorrow = new Date();
        tomorrow.setDate(tomorrow.getDate() + 1);
        const outDate = tomorrow.toISOString().split('T')[0]; // 仓库日期
        
        const [existingData] = await conn.query(
            'SELECT 1 FROM jianhuo_data WHERE 出库日期 = ? LIMIT 1',
            [outDate]
        );
        
        if (existingData.length > 0) {
            logger.info(`出库日期为${outDate}的数据已存在，无需重复写入`);
            await setWarehouseDate(outDate, conn); // 确保仓库日期已设置
            return { success: true, message: '数据已存在', outDate, count: 0 };
        }
        
        const hours = [23, 0, 1, 2, 3, 4, 5, 6];
        const values = hours.map(hour => [outDate, hour, 0, 0]);
        
        const [result] = await conn.query(
            'INSERT INTO jianhuo_data (出库日期, 拣货小时, 拣货单量, 当日应出库单量) VALUES ?',
            [values]
        );
        
        await setWarehouseDate(outDate, conn); // 设置仓库日期
        
        logger.info(`成功写入${result.affectedRows}条拣货数据，出库日期：${outDate}`);
        return { success: true, message: '数据写入成功', outDate, count: result.affectedRows };
    } catch (error) {
        logger.error(`执行jianhuo_data任务出错: ${error.message}`);
        throw error;
    } finally {
        conn.release();
    }
}

async function executeCustomerInfo() {
    const conn = await pool.getConnection();
    try {
        const baseUrl = 'https://omp.xlwms.com/gateway/omp/customer/list';
        const size = 200;
        const type = 1;
        let currentPage = 1;
        let allRecords = [];
        let totalPages = 1;

        // 首次请求获取总页数
        const firstUrl = `${baseUrl}?current=${currentPage}&size=${size}&type=${type}`;
        const firstResponse = await instance.get(firstUrl, { headers: config.externalApi.headers });

        if (firstResponse.data.code !== 200) {
            throw new Error(`接口返回错误：${firstResponse.data.msg || '首次请求失败'}`);
        }

        const firstData = firstResponse.data.data;
        allRecords = allRecords.concat(firstData.records);
        totalPages = firstData.pages;

        // 分页请求剩余数据
        if (totalPages > 1) {
            for (let page = 2; page <= totalPages; page++) {
                const url = `${baseUrl}?current=${page}&size=${size}&type=${type}`;
                const response = await instance.get(url, { headers: config.externalApi.headers });

                if (response.data.code !== 200) {
                    throw new Error(`第${page}页请求失败：${response.data.msg}`);
                }

                allRecords = allRecords.concat(response.data.data.records);
                logger.info(`已同步第${page}/${totalPages}页数据`);
            }
        }

        logger.info(`所有客户数据获取完成，共${allRecords.length}条`);

        // 格式化数据
        const formattedData = allRecords.map(record => ({
            customer_code: record.customerCode,
            customer_name: record.customerName || '',
            admin_account: record.adminAccount || '',
            company_name: record.companyName || '',
            contacter: record.contacter || '',
            address: record.address || '',
            sales_people_name: record.salesPeopleName || '',
            customer_people_name: record.customerPeopleName || '',
            create_time: record.createTime ? new Date(record.createTime) : null,
            update_time: record.updateTime ? new Date(record.updateTime) : null,
            email: record.email || '',
            telephone_no: record.telephoneNo || '',
            warehouse: record.warehouse || ''
        }));

        // 批量插入或更新
        const sql = `
            INSERT INTO customer_info (
                customer_code, customer_name, admin_account, company_name, contacter,
                address, sales_people_name, customer_people_name, create_time, update_time,
                email, telephone_no, warehouse
            ) VALUES ?
            ON DUPLICATE KEY UPDATE
                customer_name = VALUES(customer_name),
                admin_account = VALUES(admin_account),
                company_name = VALUES(company_name),
                contacter = VALUES(contacter),
                address = VALUES(address),
                sales_people_name = VALUES(sales_people_name),
                customer_people_name = VALUES(customer_people_name),
                create_time = VALUES(create_time),
                update_time = VALUES(update_time),
                email = VALUES(email),
                telephone_no = VALUES(telephone_no),
                warehouse = VALUES(warehouse)
        `;

        const values = formattedData.map(item => [
            item.customer_code,
            item.customer_name,
            item.admin_account,
            item.company_name,
            item.contacter,
            item.address,
            item.sales_people_name,
            item.customer_people_name,
            item.create_time,
            item.update_time,
            item.email,
            item.telephone_no,
            item.warehouse
        ]);

        const [result] = await conn.query(sql, [values]);
        
        return {
            success: true,
            stats: {
                total: allRecords.length,
                inserted: result.affectedRows - result.changedRows,
                updated: result.changedRows,
                pages: totalPages
            }
        };
    } catch (error) {
        logger.error(`执行customer_info任务出错: ${error.message}`);
        throw error;
    } finally {
        conn.release();
    }
}

async function executeJianhuoDataa() {
    const conn = await pool.getConnection();
    try {
        // 获取客户代码
        const [customers] = await conn.query(
            "SELECT customer_code FROM customer_info WHERE customer_name NOT LIKE 'DK%'"
        );
        const customerCodes = customers.map(c => c.customer_code).join(',');
        if (!customerCodes) {
            throw new Error('未查询到符合条件的客户代码');
        }

        // 计算时间范围
        const now = new Date();
        const startTimeDate = new Date(now);
        startTimeDate.setDate(startTimeDate.getDate() - 2);
        const startTime = `${startTimeDate.toISOString().split('T')[0]} 01:00:00`;
        
        const endTimeDate = new Date(startTimeDate);
        endTimeDate.setDate(endTimeDate.getDate() + 2);
        const endTime = `${endTimeDate.toISOString().split('T')[0]} 01:00:00`;

        // 请求待复核数
        const requestData = {
            "current": 1,
            "size": 20,
            "status": "20",
            "customerCodes": customerCodes,
            "logisticsChannel": "",
            "logisticsCarrier": "",
            "orderSourceList": [],
            "expressFlag": "",
            "varietyType": "",
            "salesPlatform": "",
            "timeType": "createTime",
            "orderNoType": "sourceNo",
            "codeType": "barcode",
            "receiver": "",
            "orderCount": "",
            "countKind": "orderWeight",
            "unitMark": 0,
            "relatedReturnOrder": "",
            "appendixFlag": "",
            "forecastStatus": "",
            "countryRegionCodes": "",
            "categoryIdList": [],
            "receiverValue": "",
            "expressPrintStatus": "",
            "withVas": "",
            "productPackType": "",
            "startTime": startTime,
            "endTime": endTime,
            "weightCountStart": "",
            "weightCountEnd": "",
            "sourceNoLists": [],
            "whCode": "NY01"
        };

        const targetUrl = 'https://omp.xlwms.com/gateway/wms/blDelivery/page';
        const response = await instance.post(targetUrl, requestData, {
            headers: config.externalApi.headers
        });

        const { code, data, msg } = response.data;
        if (code !== 200) {
            throw new Error(`接口返回错误：${msg || '未知错误'}`);
        }
        const 待复核数 = data.total || 0;

        // 获取当前小时和仓库日期
        const currentHour = new Date().getHours();
        const warehouseDate = await getWarehouseDate(conn);

        // 更新对应小时的拣货单量
        const [updateResult] = await conn.query(
            'UPDATE jianhuo_data SET 拣货单量 = ? WHERE 出库日期 = ? AND 拣货小时 = ?',
            [待复核数, warehouseDate, currentHour]
        );

        if (updateResult.affectedRows === 0) {
            throw new Error(`未找到出库日期${warehouseDate}、小时${currentHour}的记录，无法更新拣货单量`);
        }

        logger.info(`成功更新拣货单量：出库日期${warehouseDate} ${currentHour}时，待复核数=${待复核数}`);
        return { success: true, 待复核数, warehouseDate, currentHour };
    } catch (error) {
        logger.error(`执行jianhuo_dataa任务出错: ${error.message}`);
        throw error;
    } finally {
        conn.release();
    }
}

async function executeJianhuoDatab() {
    const conn = await pool.getConnection();
    try {
        // 1. 查询符合条件的客户代码：排除DK开头、XXS开头和包含test(无论大小写)的客户
        const [customers] = await conn.query(
            "SELECT customer_code FROM customer_info " +
            "WHERE customer_name NOT LIKE 'DK%' " +
            "AND customer_name NOT LIKE 'XXS%' " +
            "AND LOWER(customer_name) NOT LIKE '%test%'"
        );
        const customerCodes = customers.map(c => c.customer_code).join(',');
        if (!customerCodes) {
            throw new Error('未查询到符合条件的客户代码');
        }

        // 2. 计算时间范围
        const now = new Date();
        const currentDate = new Date(now);
        
        const startTimeDate = new Date(currentDate);
        startTimeDate.setDate(startTimeDate.getDate() - 1);
        const startTime = `${startTimeDate.toISOString().split('T')[0]} 01:00:00`;
        
        const endTimeDate = new Date(startTimeDate);
        endTimeDate.setDate(endTimeDate.getDate() + 1);
        const endTime = `${endTimeDate.toISOString().split('T')[0]} 01:00:00`;

        // 3. 循环请求各状态数据
        const statusList = ["10", "15", "20", "30", "100", "111"];
        const statusTotals = [];

        for (const status of statusList) {
            const requestData = {
                "current": 1,
                "size": 20,
                "status": status,
                "customerCodes": customerCodes,
                "logisticsChannel": "",
                "logisticsCarrier": "",
                "orderSourceList": [],
                "expressFlag": "",
                "varietyType": "",
                "salesPlatform": "",
                "timeType": "createTime",
                "orderNoType": "sourceNo",
                "codeType": "barcode",
                "receiver": "",
                "orderCount": "",
                "countKind": "orderWeight",
                "unitMark": 0,
                "relatedReturnOrder": "",
                "appendixFlag": "",
                "forecastStatus": "",
                "countryRegionCodes": "",
                "categoryIdList": [],
                "receiverValue": "",
                "expressPrintStatus": "",
                "withVas": "",
                "productPackType": "",
                "startTime": startTime,
                "endTime": endTime,
                "weightCountStart": "",
                "weightCountEnd": "",
                "sourceNoLists": [],
                "whCode": "NY01"
            };

            const targetUrl = 'https://omp.xlwms.com/gateway/wms/blDelivery/page';
            const response = await instance.post(targetUrl, requestData, {
                headers: config.externalApi.headers
            });

            const { code, data, msg } = response.data;
            if (code !== 200) {
                throw new Error(`状态码${status}请求失败：${msg || '未知错误'}`);
            }
            const total = data.total || 0;
            statusTotals.push({ status, total });
        }

        // 4. 计算今日应出库订单总数
        const 今日应出库订单总数 = statusTotals.reduce((sum, item) => sum + item.total, 0);
        
        // 5. 获取仓库日期并更新数据库
        const warehouseDate = await getWarehouseDate(conn);
        const [updateResult] = await conn.query(
            'UPDATE jianhuo_data SET 当日应出库单量 = ? WHERE 出库日期 = ?',
            [今日应出库订单总数, warehouseDate]
        );

        if (updateResult.affectedRows === 0) {
            throw new Error(`未找到出库日期为${warehouseDate}的记录，无法更新当日应出库单量`);
        }

        logger.info(`成功更新${updateResult.affectedRows}条记录的当日应出库单量：${今日应出库订单总数}`);
        return { 
            success: true, 
            今日应出库订单总数, 
            warehouseDate,
            updatedRows: updateResult.affectedRows
        };
    } catch (error) {
        logger.error(`执行jianhuo_datab任务出错: ${error.message}`);
        throw error;
    } finally {
        conn.release();
    }
}


// 新增定时任务：每天24:00访问 /getALLResultTest?updateTestRates=True 接口   获取客户余额
cron.schedule('1 0 * * *', async () => { // cron表达式含义：每天0点0分（即24:00）执行
        try {
        const baseUrl = 'https://omp.xlwms.com/gateway/omp/customer/list';
        const size = 50;
        let currentPage = 1;
        let allRecords = [];
        let total = 0;

        // 首次请求获取总数据量
        const firstUrl = `${baseUrl}?current=${currentPage}&size=${size}&type=1&customerQuery=`;
        const firstResponse = await instance.get(firstUrl, { headers: config.externalApi.headers });

        if (firstResponse.status!== 200) {
            throw new Error(`首次请求失败，状态码: ${firstResponse.status}`);
        }

        total = firstResponse.data.data.total;
        const pageCount = Math.ceil(total / size);

        allRecords = allRecords.concat(firstResponse.data.data.records);

        // 后续分页请求
        for (let i = 2; i <= pageCount; i++) {
            const url = `${baseUrl}?current=${i}&size=${size}&type=1&customerQuery=`;
            const response = await instance.get(url, { headers: config.externalApi.headers });

            if (response.status!== 200) {
                throw new Error(`第 ${i} 页请求失败，状态码: ${response.status}`);
            }

            allRecords = allRecords.concat(response.data.data.records);
        }

        const result = allRecords
            .filter(record => {
                const customerName = (record.customerName || '').toLowerCase();
                const customerCode = (record.customerCode || '').toLowerCase();
                // 过滤掉客户名称以 DK 开头以及客户名称或代码包含 test 的记录
                return!record.customerName.startsWith('DK') &&!customerName.includes('test') &&!customerCode.includes('test');
            })
            .map(record => {
                const usdBalance = record.holdValues.find(item => item.currencyCode === 'USD')?.amount || '0.0000';
                const usdCredit = record.creditValues.find(item => item.currencyCode === 'USD')?.amount || '0.0000';
                // 计算总余额
                const totalBalance = (parseFloat(usdBalance) + parseFloat(usdCredit)).toFixed(4);
                return {
                    客户: record.customerName,
                    客户代码: record.customerCode,
                    余额: usdBalance,
                    信用额度: usdCredit,
                    总余额: totalBalance
                };
            });

        // 获取当前日期
        const currentDate = new Date().toISOString().split('T')[0];

        // 读取 data.json 文件
        const dataFilePath = path.join(__dirname, 'data.json');
        let historicalData = [];
        try {
            if (fs.existsSync(dataFilePath)) {
                const data = fs.readFileSync(dataFilePath, 'utf8');
                if (data.trim()) {
                    historicalData = JSON.parse(data);
                    logger.info(`成功读取 data.json 文件，包含 ${historicalData.length} 条历史数据`);
                }
            }
        } catch (parseError) {
            logger.error(`解析 data.json 文件失败: ${parseError.message}，将尝试保留原始数据`);
            // 尝试保留原始数据，避免重置
            try {
                historicalData = [JSON.parse(data)];
            } catch (retryError) {
                logger.error(`再次解析 data.json 文件失败: ${retryError.message}，将重置文件内容`);
                historicalData = [];
            }
        }

        // 添加新数据
        historicalData.push({
            date: currentDate,
            data: result
        });

        // 只保留近十天的数据
        historicalData = historicalData.slice(-10);
        // 将数据写入 data.json 文件
        try {
            fs.writeFileSync(dataFilePath, JSON.stringify(historicalData, null, 2));
            logger.info('定时任务执行成功，数据已更新到 data.json');
        } catch (writeError) {
            logger.error(`写入 data.json 文件失败: ${writeError.message}`);
        }

        
    } catch (error) {
        logger.error(`定时任务执行失败: ${error.message}`);
    }
});
// 定时任务：每小时执行一次数据获取和写入，包含物流商判断逻辑
cron.schedule('0 * * * *', async () => {
    try {
        const data = await fetchData();
        await writeData(data);
        
        // ========== 调用 /ruku 接口 ==========
        const rukuUrl = `http://localhost:${port}/ruku`;
        logger.info(`定时任务准备调用 /ruku 接口: ${rukuUrl}`);
        const rukuResponse = await axios.get(rukuUrl);
        logger.info(`定时任务调用 /ruku 接口成功`);
        // ========== 调用 /ruku 接口结束 ==========

        // ========== 同步 aoyu_data 到 wuliu_data（包含仓库字段，以出库单号为唯一值） ==========
        const now = new Date();
        const startTime = new Date(now);
        startTime.setDate(now.getDate() - 14);
        startTime.setHours(0, 0, 0, 0);

        const endTime = new Date(now);
        endTime.setDate(now.getDate());
        endTime.setHours(23, 59, 59, 999);

        const startTimeStr = aaa(startTime);
        const endTimeStr = aaa(endTime);

        // 获取数据库连接
        const conn = await pool.getConnection();
        try {
            // 步骤1：查询aoyu_data，新增出库单号非空过滤（唯一值字段必须非空）
            const selectSql = `
                SELECT 
                    客户名称, 
                    跟踪单号, 
                    物流渠道, 
                    物流渠道组, 
                    出库时间,
                    出库单号,  -- 关键：出库单号作为唯一值
                    仓库  
                FROM aoyu_data
                WHERE 
                    出库时间 BETWEEN ? AND ?
                    AND 客户名称 IS NOT NULL AND 客户名称 != ''
                    AND 物流渠道 IS NOT NULL AND 物流渠道 != ''
                    AND 出库时间 IS NOT NULL
                    AND 仓库 IS NOT NULL AND 仓库 != ''
                    AND 出库单号 IS NOT NULL AND 出库单号 != ''  -- 新增：过滤出库单号为空的数据
            `;

            logger.info(`准备从aoyu_data查询数据（含仓库），时间范围: ${startTimeStr} 到 ${endTimeStr}`);
            const [aoyuData] = await conn.query(selectSql, [startTimeStr, endTimeStr]);
            logger.info(`从aoyu_data查询到 ${aoyuData.length} 条含仓库的符合条件数据`);

            if (aoyuData.length === 0) {
                logger.info('没有需要同步的含仓库数据');
            } else {
                // 步骤2：构建【出库单号】数组（原逻辑是跟踪单号，这里修改）
                const outBoundOrderNumbers = aoyuData.map(item => item.出库单号);

                // 步骤3：查询wuliu_data中已存在的【出库单号】（判断唯一值）
                const existingSql = `
                    SELECT 出库单号 
                    FROM wuliu_data 
                    WHERE 出库单号 IN (?)
                `;

                const [existingData] = await conn.query(existingSql, [outBoundOrderNumbers]);
                const existingOutBoundNumbers = existingData.map(item => item.出库单号);
                // logger.info(`wuliu_data中已存在 ${existingOutBoundNumbers.length} 条匹配的出库单号`);

                // 步骤4：过滤已存在的记录（用出库单号判断）
                const newData = aoyuData.filter(item => !existingOutBoundNumbers.includes(item.出库单号));
                // logger.info(`将插入 ${newData.length} 条含仓库的新记录到wuliu_data`);

                if (newData.length > 0) {
                    // 步骤5：插入wuliu_data，修正出库单号映射
                    const insertSql = `
                        INSERT INTO wuliu_data (
                            客户名称, 
                            跟踪单号, 
                            渠道, 
                            渠道组, 
                            出库时间,
                            出库单号,  -- 目标字段：出库单号
                            仓库  
                        ) VALUES ?
                    `;

                    // 映射关系：修正出库单号的来源（原逻辑错把仓库当作出库单号）
                    const values = newData.map(item => [
                        item.客户名称,
                        item.跟踪单号,
                        item.物流渠道,
                        item.物流渠道组,
                        item.出库时间,
                        item.出库单号,  // 正确映射：aoyu_data.出库单号 → wuliu_data.出库单号
                        item.仓库        // 仓库字段单独映射
                    ]);

                    await conn.query(insertSql, [values]);
                    // logger.info(`成功插入 ${newData.length} 条含仓库的记录到wuliu_data`);
                }
            }
        } catch (error) {
            logger.error(`同步数据（含仓库）失败: ${error.stack}`);
        } finally {
            conn.release(); // 释放连接
        }
        // ========== 同步 aoyu_data 到 wuliu_data 结束 ==========

        // ========== 新增：物流商判断并写入数据库 ==========
        try {
            // 重新获取数据库连接
            const conn = await pool.getConnection();
            try {
                // 查询所有需要判断物流商的记录，同时获取跟踪单号和渠道
                const logisticsSql = `
                    SELECT 
                        id, 
                        跟踪单号,
                        渠道
                    FROM wuliu_data 
                    WHERE 
                        物流商 IS NULL 
                        AND 出库时间 BETWEEN ? AND ?
                `;
                
                logger.info(`准备查询需要判断物流商的记录，时间范围: ${startTimeStr} 到 ${endTimeStr}`);
                const [logisticsData] = await conn.query(logisticsSql, [startTimeStr, endTimeStr]);
                // logger.info(`找到 ${logisticsData.length} 条需要判断物流商的记录`);

                if (logisticsData.length === 0) {
                    // logger.info('没有需要判断物流商的记录');
                } else {
                    // 定义物流商判断规则
                    const logisticsProviderRules = [
                        { regex: /^TBA/i, provider: 'amazon' },
                        { regex: /^GF/i, provider: 'gofo' },
                        { regex: /^UU[A-Z0-9]{10,20}$/i, provider: 'uniuni' },
                        { regex: /^\d{12,20}$/, provider: 'fedex' },
                        { regex: /^1Z[A-HJ-NP-Z0-9]{16}$/, provider: 'ups' },
                        { regex: /^(91|92|93|94|95|96|03|04|70|23|02)\d{18,22}$/, provider: 'usps' }
                    ];

                    // 批量更新物流商信息
                    const updatePromises = logisticsData.map(item => {
                        const trackingNumber = item.跟踪单号 || '';
                        const channel = item.渠道 || '';
                        let provider = '未知物流商';
                        
                        // 根据跟踪单号判断物流商
                        for (const rule of logisticsProviderRules) {
                            if (rule.regex.test(trackingNumber)) {
                                // 特殊处理：如果匹配到USPS，还需要检查渠道字段
                                if (rule.provider === 'usps' && channel.toLowerCase().includes('amazon')) {
                                    provider = 'amazon';
                                } else {
                                    provider = rule.provider;
                                }
                                break;
                            }
                        }

                        // 更新数据库记录
                        const updateSql = `
                            UPDATE wuliu_data 
                            SET 物流商 = ? 
                            WHERE id = ?
                        `;
                        
                        return conn.query(updateSql, [provider, item.id]);
                    });

                    // 执行所有更新操作
                    await Promise.all(updatePromises);
                    // logger.info(`成功更新 ${logisticsData.length} 条记录的物流商信息`);
                }
            } catch (error) {
                logger.error(`物流商判断并写入数据库失败: ${error.stack}`);
            } finally {
                conn.release(); // 释放连接
            }
        } catch (error) {
            logger.error(`获取数据库连接失败: ${error.stack}`);
        }
        // ========== 物流商判断并写入数据库结束 ==========

        // 原有的更新单量数据逻辑
        const apiUrl = `http://localhost:${port}/a?startTime=${encodeURIComponent(startTimeStr)}&endTime=${encodeURIComponent(endTimeStr)}`;
        logger.info(`准备请求接口: ${apiUrl}`);
        const responseA = await axios.get(apiUrl);
        logger.info(`定时任务执行成功，访问 /a 接口，响应: ${JSON.stringify(responseA.data)}`);
        logger.info('定时任务执行成功，数据已获取并写入');
    } catch (error) {
        logger.error(`定时任务执行失败: ${error.stack}`);
    }
});    
// // 新增：每周一早上9点执行的定时任务
// cron.schedule('7 17 * * 4', async () => { 
//   logger.info('开始执行订单分析定时任务');
//   await fetchAndSendOrderAnalysis();
// });

const fetchSkuKcData = async () => {
    try {
        const response = await axios.get('https://wdbso.vip/sku_kc', {
            timeout: 300000, // 设置30秒超时
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
            }
        });

        // 可根据实际需要处理响应数据（示例仅记录日志）
        logger.info(`访问 https://wdbso.vip/sku_kc 成功，响应状态码：${response.status}`);
        // 如果需要存储响应数据，可添加文件写入或数据库存储逻辑
        // await fs.promises.writeFile('./sku_kc_response.json', JSON.stringify(response.data));
    } catch (error) {
        logger.error(`访问 https://wdbso.vip/sku_kc 失败: ${error.message}`, {
            status: error.response?.status,
            data: error.response?.data
        });
    }
};
// USPS物流更新函数（新增妥投时间处理）
const concurrentUpdateUspsLogistics = async (concurrency = 1000, filteredIds = []) => {
    // logger.info(`开始USPS物流更新，并发数: ${concurrency}，筛选ID: ${filteredIds.length > 0 ? filteredIds.join(',') : '无'}`);
    const conn = await pool.getConnection();
    try {
        const baseSql = `
            SELECT id, 跟踪单号, 物流状态 AS originalStatus 
            FROM wuliu_data 
            WHERE 物流商 = 'usps' 
            AND (物流状态 IS NULL OR 物流状态 != '已签收')
        `;
        
        const sql = filteredIds.length > 0 ? `${baseSql} AND id IN (?)` : baseSql;
        const queryParams = filteredIds.length > 0 ? [filteredIds] : [];
        
        const [orders] = await conn.query(sql, queryParams);
        // logger.info(`找到 ${orders.length} 条需要更新的USPS订单`);
        if (orders.length === 0) return { success: true, count: 0, logistics: 'USPS' };
        
        const updateSingleOrder = async (order) => {
            try {
                const { id, 跟踪单号: trackingNumber, originalStatus } = order;
                const response = await axios.get(`https://wdbso.vip/get_usps?number=${trackingNumber}`, {
                    headers: { 'token': '3d71e96301854e1975b8a714059e9bb3712a0bb6a0bfd3ab652835b0e12f00ea' },
                    timeout: 30000
                });
                
                if (!response.data.success || !response.data.data || !response.data.data.data) {
                    throw new Error(`接口返回异常`);
                }
                
                const data = response.data.data.data;
                const tracks = data.info || [];
                
                const statusMap = {
                    'In Transit': '运输中',
                    'Out for Delivery': '派送中',
                    'Delivered': '已签收',
                    'Created Order': '订单创建',
                    'Exception': '异常'
                };
                const logisticsStatus = statusMap[data.status] || '等待同步';
                
                // 【新增：判断是否为已签收状态，提取妥投时间】
                let signTime = null;
                if (logisticsStatus === '已签收') {
                    // USPS最后一条轨迹时间为妥投时间
                    signTime = tracks[tracks.length - 1]?.date || null; 
                }
                
                const isNewStatusUnknown = logisticsStatus === '等待同步';
                const isOriginalStatusValid = originalStatus && 
                                            originalStatus !== '等待同步' && 
                                            originalStatus !== '等待同步';
                
                if (isNewStatusUnknown && isOriginalStatusValid) {
                    // logger.info(`USPS保留原数据：${trackingNumber}（原状态：${originalStatus}）`);
                    return { success: true, trackingNumber, skipped: true, logistics: 'USPS' };
                }
                
                let onlineTime = null;
                for (const track of tracks) {
                    if (track.status === 'In Transit') {
                        onlineTime = track.date;
                    }
                }
                
                // 【修改：更新语句新增妥投时间字段】
                const updateSql = `
                    UPDATE wuliu_data 
                    SET 上网时间 = ?, 最新轨迹详情 = ?, 最新轨迹时间 = ?, 物流状态 = ?, 
                        妥投时间 = ?  -- 新增：妥投时间
                    WHERE id = ?
                `;
                await conn.query(updateSql, [
                    onlineTime,
                    tracks[0]?.event || null,
                    tracks[0]?.date || null,
                    logisticsStatus,
                    signTime,  // 传入妥投时间
                    id
                ]);
                
                // logger.info(`USPS更新成功：${trackingNumber} → ${logisticsStatus}（妥投时间：${signTime || '无'}）`);
                return { success: true, trackingNumber, logistics: 'USPS' };
                
            } catch (error) {
                logger.error(`USPS更新失败：${order.跟踪单号}，错误：${error.message}`);
                await conn.query(`UPDATE wuliu_data SET 物流状态 = '等待同步', 妥投时间 = null WHERE id = ?`, [order.id]);
                return { success: false, trackingNumber: order.跟踪单号, error: error.message, logistics: 'USPS' };
            }
        };
        
        // 分块并发处理（保持不变）
        const results = [];
        for (let i = 0; i < orders.length; i += concurrency) {
            const chunk = orders.slice(i, i + concurrency);
            const chunkResults = await Promise.allSettled(chunk.map(updateSingleOrder));
            results.push(...chunkResults);
            if (orders.length > concurrency) await new Promise(resolve => setTimeout(resolve, 500));
        }
        
        const totalSuccess = results.filter(r => r.status === 'fulfilled' && r.value.success).length;
        const totalSkipped = results.filter(r => r.status === 'fulfilled' && r.value.skipped).length;
        const totalFailed = results.length - totalSuccess - totalSkipped;
        
        // logger.info(`USPS更新完成：成功=${totalSuccess}，跳过=${totalSkipped}，失败=${totalFailed}`);
        return { 
            success: true, 
            total: results.length, 
            success: totalSuccess, 
            skipped: totalSkipped, 
            failed: totalFailed,
            logistics: 'USPS'
        };
        
    } catch (error) {
        logger.error(`USPS更新任务失败：${error.message}`);
        return { success: false, error: error.message, logistics: 'USPS' };
    } finally {
        conn.release();
    }
};


// UNIUNI物流更新函数（新增妥投时间处理）
const concurrentUpdateUniuniLogistics = async (concurrency = 1000, filteredIds = []) => {
    // logger.info(`开始UNIUNI物流更新，并发数: ${concurrency}，筛选ID: ${filteredIds.length > 0 ? filteredIds.join(',') : '无'}`);
    const conn = await pool.getConnection();
    try {
        const baseSql = `
            SELECT id, 跟踪单号, 物流状态 AS originalStatus 
            FROM wuliu_data 
            WHERE 物流商 = 'uniuni' 
            AND (物流状态 IS NULL OR 物流状态 != '已签收')
        `;
        
        const sql = filteredIds.length > 0 ? `${baseSql} AND id IN (?)` : baseSql;
        const queryParams = filteredIds.length > 0 ? [filteredIds] : [];
        
        const [orders] = await conn.query(sql, queryParams);
        // logger.info(`找到 ${orders.length} 条需要更新的UNIUNI订单`);
        if (orders.length === 0) return { success: true, count: 0, logistics: 'UNIUNI' };
        
        const updateSingleOrder = async (order) => {
            try {
                const { id, 跟踪单号: trackingNumber, originalStatus } = order;
                const response = await axios.get(`https://wdbso.vip/get_uniuni?id=${trackingNumber}`, {
                    headers: { 'token': '3d71e96301854e1975b8a714059e9bb3712a0bb6a0bfd3ab652835b0e12f00ea' },
                    timeout: 30000
                });
                
                if (!response.data.success || !response.data.data || !response.data.data.data) {
                    throw new Error(`接口返回异常`);
                }
                
                const data = response.data.data.data;
                const validTno = data.valid_tno.find(item => item.tno === trackingNumber);
                if (!validTno) throw new Error(`未找到有效数据`);
                
                const tracks = validTno.spath_list || [];
                const state = validTno.state;
                
                const statusMap = {
                    '190': '订单创建', '199': '运输中', '195': '运输中', '200': '到达扫描',
                    '202': '派送中', '203': '已签收', '7': '异常', '8': '已退回', '9': '待取件', '255': '运输中', '219':'运输中', '213':'妥投失败', '231':'妥投失败', '211':'妥投失败'
                };
                const logisticsStatus = statusMap[state.toString()] || '等待同步';
                
                // 【新增：判断已签收状态，提取妥投时间】
                let signTime = null;
                if (logisticsStatus === '已签收') {
                    // UNIUNI最后一条轨迹的localTime为妥投时间
                    const lastTrack = tracks[tracks.length - 1] || {};
                    signTime = lastTrack.dateTime?.localTime || null;
                }
                
                const isNewStatusUnknown = logisticsStatus === '等待同步';
                const isOriginalStatusValid = originalStatus && 
                                            originalStatus !== '等待同步' && 
                                            originalStatus !== '等待同步';
                
                if (isNewStatusUnknown && isOriginalStatusValid) {
                    // logger.info(`UNIUNI保留原数据：${trackingNumber}（原状态：${originalStatus}）`);
                    return { success: true, trackingNumber, skipped: true, logistics: 'UNIUNI' };
                }
                
                let onlineTime = null;
                const transitStates = ['199', '195', '255'];
                for (const track of tracks) {
                    if (transitStates.includes(track.state.toString())) {
                        onlineTime = track.dateTime?.localTime || null;
                        break;
                    }
                }
                
                const lastTrack = tracks[tracks.length - 1] || {};
                const lastTrackDetail = lastTrack.pathAddress || null;
                const lastTrackTime = lastTrack.dateTime?.localTime || null;
                
                // 【修改：更新语句新增妥投时间字段】
                const updateSql = `
                    UPDATE wuliu_data 
                    SET 上网时间 = ?, 最新轨迹详情 = ?, 最新轨迹时间 = ?, 物流状态 = ?,
                        妥投时间 = ?  -- 新增：妥投时间
                    WHERE id = ?
                `;
                await conn.query(updateSql, [
                    onlineTime,
                    lastTrackDetail,
                    lastTrackTime,
                    logisticsStatus,
                    signTime,  // 传入妥投时间
                    id
                ]);
                
                // logger.info(`UNIUNI更新成功：${trackingNumber} → ${logisticsStatus}（妥投时间：${signTime || '无'}）`);
                return { success: true, trackingNumber, logistics: 'UNIUNI' };
                
            } catch (error) {
                // logger.error(`UNIUNI更新失败：${order.跟踪单号}，错误：${error.message}`);
                await conn.query(`UPDATE wuliu_data SET 物流状态 = '等待同步', 妥投时间 = null WHERE id = ?`, [order.id]);
                return { success: false, trackingNumber: order.跟踪单号, error: error.message, logistics: 'UNIUNI' };
            }
        };
        
        // 分块并发处理（保持不变）
        const results = [];
        for (let i = 0; i < orders.length; i += concurrency) {
            const chunk = orders.slice(i, i + concurrency);
            const chunkResults = await Promise.allSettled(chunk.map(updateSingleOrder));
            results.push(...chunkResults);
            if (orders.length > concurrency) await new Promise(resolve => setTimeout(resolve, 500));
        }
        
        const totalSuccess = results.filter(r => r.status === 'fulfilled' && r.value.success).length;
        const totalSkipped = results.filter(r => r.status === 'fulfilled' && r.value.skipped).length;
        const totalFailed = results.length - totalSuccess - totalSkipped;
        
        // logger.info(`UNIUNI更新完成：成功=${totalSuccess}，跳过=${totalSkipped}，失败=${totalFailed}`);
        return { 
            success: true, 
            total: results.length, 
            success: totalSuccess, 
            skipped: totalSkipped, 
            failed: totalFailed,
            logistics: 'UNIUNI'
        };
        
    } catch (error) {
        logger.error(`UNIUNI更新任务失败：${error.message}`);
        return { success: false, error: error.message, logistics: 'UNIUNI' };
    } finally {
        conn.release();
    }
};

// FedEx物流更新函数（优化妥投时间逻辑）
const concurrentUpdateFedExLogistics = async (concurrency = 1000, filteredIds = []) => {
    // logger.info(`开始FedEx物流更新，并发数: ${concurrency}，筛选ID: ${filteredIds.length > 0 ? filteredIds.join(',') : '无'}`);
    const conn = await pool.getConnection();
    try {
        const baseSql = `
            SELECT id, 跟踪单号, 物流状态 AS originalStatus 
            FROM wuliu_data 
            WHERE 物流商 = 'fedex' 
            AND (物流状态 IS NULL OR 物流状态 != '已签收')
        `;
        
        const sql = filteredIds.length > 0 ? `${baseSql} AND id IN (?)` : baseSql;
        const queryParams = filteredIds.length > 0 ? [filteredIds] : [];
        
        const [orders] = await conn.query(sql, queryParams);
        // logger.info(`找到 ${orders.length} 条需要更新的FedEx订单`);
        if (orders.length === 0) return { success: true, count: 0, logistics: 'fedex' };
        
        const updateSingleOrder = async (order) => {
            try {
                const { id, 跟踪单号: trackingNumber, originalStatus } = order;
                const response = await axios.post(
                    'https://wdbso.vip/get_wuliu',
                    { trackingNumbers: [trackingNumber] },
                    {
                        headers: { 'token': '3d71e96301854e1975b8a714059e9bb3712a0bb6a0bfd3ab652835b0e12f00ea', 'Content-Type': 'application/json' },
                        timeout: 30000
                    }
                );
                
                if (!response.data.success || !response.data.data || !response.data.data.data) {
                    throw new Error(`FedEx接口返回异常`);
                }
                
                const data = response.data.data.data;
                const fedexData = data.find(item => item.trackingNumber === trackingNumber);
                if (!fedexData) throw new Error(`未找到跟踪号 ${trackingNumber} 的有效数据`);
                
                const tracks = fedexData.trackDetailDTOS || [];
                if (tracks.length === 0) throw new Error(`无轨迹数据`);
                
                const statusMap = {
                    '已送达': '已签收', '正在运送': '运输中', '已收取': '已揽收', '标签已创建': '订单创建',
                    '递送已更新': '运输中', '货件已到达当地配送中心': '运输中', '已安排递送': '运输中',
                    '货件将准时送达': '运输中', '已离开FedEx服务站': '运输中', '抵达FedEx服务站': '运输中'
                };
                
                const formatDateTime = (dateTimeStr) => {
                    if (!dateTimeStr) return null;
                    const match = dateTimeStr.match(/^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/);
                    return match ? match[1] : null;
                };
                
                const latestTrack = tracks[0];
                const derivedStatus = latestTrack.derivedStatus || 
                                     (latestTrack.statusMessage || '').match(/【([^】]+)】/)?.[1] || 
                                     '等待同步';
                const logisticsStatus = statusMap[derivedStatus] || '等待同步';
                
                // 【优化：未找到"已送达"轨迹时，妥投时间明确设为null】
                let signTime = null;
                if (logisticsStatus === '已签收') {
                    const deliveredTrack = tracks.find(track => 
                        (track.derivedStatus === '已送达') || 
                        (track.statusMessage && track.statusMessage.includes('【已送达】'))
                    );
                    
                    if (deliveredTrack) {
                        signTime = formatDateTime(deliveredTrack.startTime);
                    } else {
                        logger.warn(`FedEx标记为"已签收"，但未找到"已送达"轨迹：${trackingNumber}`);
                        // 未找到时，signTime保持null
                    }
                }
                
                const isNewStatusUnknown = logisticsStatus === '等待同步';
                const isOriginalStatusValid = originalStatus && 
                                            originalStatus !== '等待同步' && 
                                            originalStatus !== '未知';
                
                if (isNewStatusUnknown && isOriginalStatusValid) {
                    // logger.info(`FedEx保留原数据：${trackingNumber}（原状态：${originalStatus}）`);
                    return { success: true, trackingNumber, skipped: true, logistics: 'fedex' };
                }
                
                let onlineTime = null;
                const validOnlineStatus = ['正在运送', '已收取'];
                for (const track of tracks) {
                    const status = track.derivedStatus || (track.statusMessage || '').match(/【([^】]+)】/)?.[1];
                    if (validOnlineStatus.includes(status)) {
                        onlineTime = formatDateTime(track.startTime);
                        break;
                    }
                }
                
                const latestTrackDetail = latestTrack.statusMessage || null;
                const latestTrackTime = formatDateTime(latestTrack.startTime);
                
                const updateSql = `
                    UPDATE wuliu_data 
                    SET 上网时间 = ?, 最新轨迹详情 = ?, 最新轨迹时间 = ?, 物流状态 = ?,
                        妥投时间 = ?
                    WHERE id = ?
                `;
                await conn.query(updateSql, [
                    onlineTime,
                    latestTrackDetail,
                    latestTrackTime,
                    logisticsStatus,
                    signTime,
                    id
                ]);
                
                // logger.info(`FedEx更新成功：${trackingNumber} → ${logisticsStatus}（妥投时间：${signTime || '无'}）`);
                return { success: true, trackingNumber, logistics: 'fedex' };
                
            } catch (error) {
                logger.error(`FedEx更新失败：${order.跟踪单号}，错误：${error.message}`);
                try {
                    await conn.query(`UPDATE wuliu_data SET 物流状态 = '等待同步', 妥投时间 = null WHERE id = ?`, [order.id]);
                } catch (dbError) {
                    logger.error(`设置等待同步失败：${dbError.message}`);
                }
                return { success: false, trackingNumber: order.跟踪单号, error: error.message, logistics: 'fedex' };
            }
        };
        
        // 分块并发处理（保持不变）
        const processChunk = async (chunk) => {
            const results = await Promise.allSettled(chunk.map(updateSingleOrder));
            const successes = results.filter(r => r.status === 'fulfilled' && r.value.success).length;
            logger.info(`FedEx批次完成：成功=${successes}，失败=${chunk.length - successes}`);
            return results;
        };
        
        const chunks = [];
        for (let i = 0; i < orders.length; i += concurrency) {
            chunks.push(orders.slice(i, i + concurrency));
        }
        
        const allResults = [];
        for (const chunk of chunks) {
            allResults.push(...await processChunk(chunk));
            if (orders.length > concurrency) await new Promise(resolve => setTimeout(resolve, 500));
        }
        
        const totalSuccess = allResults.filter(r => r.status === 'fulfilled' && r.value.success).length;
        const totalSkipped = allResults.filter(r => r.status === 'fulfilled' && r.value.skipped).length;
        const totalFailed = allResults.length - totalSuccess - totalSkipped;
        
        // logger.info(`FedEx更新完成：成功=${totalSuccess}，跳过=${totalSkipped}，失败=${totalFailed}`);
        return { 
            success: true, 
            total: allResults.length, 
            success: totalSuccess, 
            skipped: totalSkipped, 
            failed: totalFailed,
            logistics: 'fedex'
        };
        
    } catch (error) {
        logger.error(`FedEx更新任务失败：${error.message}`);
        return { success: false, error: error.message, logistics: 'fedex' };
    } finally {
        conn.release();
    }
};

// GOFO物流更新函数（修正上网时间提取）
const concurrentUpdateGofoLogistics = async (concurrency = 1000, filteredIds = []) => {
    // logger.info(`开始GOFO物流更新，并发数: ${concurrency}，筛选ID: ${filteredIds.length > 0 ? filteredIds.join(',') : '无'}`);
    const conn = await pool.getConnection();
    try {
        const baseSql = `
            SELECT id, 跟踪单号, 物流状态 AS originalStatus 
            FROM wuliu_data 
            WHERE 物流商 = 'gofo' 
            AND (物流状态 IS NULL OR 物流状态 != '已签收')
        `;
        
        const sql = filteredIds.length > 0 ? `${baseSql} AND id IN (?)` : baseSql;
        const queryParams = filteredIds.length > 0 ? [filteredIds] : [];
        
        const [orders] = await conn.query(sql, queryParams);
        // logger.info(`找到 ${orders.length} 条需要更新的GOFO订单`);
        if (orders.length === 0) return { success: true, count: 0, logistics: 'GOFO' };
        
        const updateSingleOrder = async (order) => {
            try {
                const { id, 跟踪单号: trackingNumber, originalStatus } = order;
                // 调用GOFO接口
                const response = await axios.post(
                    `https://wdbso.vip/get_gofo?id=${trackingNumber}`,
                    {},
                    {
                        headers: { 
                            'token': '3d71e96301854e1975b8a714059e9bb3712a0bb6a0bfd3ab652835b0e12f00ea',
                            'Content-Type': 'application/json'
                        },
                        timeout: 30000
                    }
                );
                
                // 验证接口返回
                if (!response.data.success || !response.data.data || !response.data.data.data || response.data.data.data.length === 0) {
                    throw new Error(`接口返回异常或无数据`);
                }
                
                const data = response.data.data.data[0];
                const tracks = data.trackEventList || [];
                // 注意：接口返回的trackEventList是按时间倒序排列（最新的轨迹在最前面）
                // 因此需要先反转数组，按时间正序处理（最早的轨迹在最前面）
                const sortedTracks = [...tracks].reverse(); 
                
                // 状态映射（保持不变）
                const statusMap = {
                    '100': '已创建', '200': '离开配送中心', '201': '到达当地设施',
                    '202': '到达配送中心', '205': '已送达', '208': '派送中',
                    '203': '离开当地设施', '412': '到达处理中心',
                    'Delivered': '已送达', 'Out for delivery': '派送中',
                    'Transit': '运输中', 'Processing': '已创建','Alert':'妥投失败','204':'运输中'
                };
                
                // 内部状态映射（保持不变）
                const internalStatusMap = {
                    '已创建': '订单创建', '离开配送中心': '运输中', '到达当地设施': '运输中',
                    '到达配送中心': '运输中', '已送达': '已签收', '派送中': '派送中',
                    '离开当地设施': '运输中', '到达处理中心': '运输中', '运输中': '运输中','妥投失败':'妥投失败'
                };
                
                // 最新轨迹（取倒序后的第一条，即原数组的最后一条）
                const lastTrack = tracks[0] || {};
                let logisticsStatus;
                if (lastTrack.processCode && statusMap[lastTrack.processCode]) {
                    logisticsStatus = statusMap[lastTrack.processCode];
                } else {
                    logisticsStatus = statusMap[data.status] || '等待同步';
                }
                const internalStatus = internalStatusMap[logisticsStatus] || '等待同步';
                
                // 提取妥投时间（保持不变）
                let signTime = null;
                if (internalStatus === '已签收') {
                    const deliveredTrack = sortedTracks.find(track => track.processCode === '205');
                    if (deliveredTrack) {
                        signTime = formatGofoDateTime(deliveredTrack.processDate);
                    } else {
                        logger.warn(`GOFO标记为"已签收"，但未找到205状态码：${trackingNumber}`);
                    }
                }
                
                // 修正：提取上网时间（第一条运输中状态的时间）
                let onlineTime = null;
                // 运输中状态码：200（离开配送中心）、201（到达当地设施）、202（到达配送中心）、203（离开当地设施）、412（到达处理中心）
                const transitCodes = ['200', '201', '202', '203', '412'];
                // 在正序轨迹中查找第一条符合运输中状态的轨迹
                const firstTransitTrack = sortedTracks.find(track => transitCodes.includes(track.processCode));
                if (firstTransitTrack) {
                    onlineTime = formatGofoDateTime(firstTransitTrack.processDate);
                }
                
                // 最新轨迹详情和时间（使用原数组的第一条，即最新的轨迹）
                const lastTrackDetail = lastTrack.processContent || null;
                const lastTrackTime = formatGofoDateTime(lastTrack.processDate);
                
                // 更新数据库
                const updateSql = `
                    UPDATE wuliu_data 
                    SET 上网时间 = ?, 最新轨迹详情 = ?, 最新轨迹时间 = ?, 物流状态 = ?,
                        妥投时间 = ?
                    WHERE id = ?
                `;
                await conn.query(updateSql, [
                    onlineTime,
                    lastTrackDetail,
                    lastTrackTime,
                    internalStatus,
                    signTime,
                    id
                ]);
                
                // logger.info(`GOFO更新成功：${trackingNumber} → ${internalStatus}（上网时间：${onlineTime || '无'}，妥投时间：${signTime || '无'}）`);
                return { success: true, trackingNumber, logistics: 'GOFO' };
                
            } catch (error) {
                logger.error(`GOFO更新失败：${trackingNumber}，错误：${error.message}`);
                await conn.query(`UPDATE wuliu_data SET 物流状态 = '等待同步', 妥投时间 = null WHERE id = ?`, [id]);
                return { success: false, trackingNumber, error: error.message };
            }
        };
        
        // 分块并发处理（保持不变）
        const results = [];
        for (let i = 0; i < orders.length; i += concurrency) {
            const chunk = orders.slice(i, i + concurrency);
            const chunkResults = await Promise.allSettled(chunk.map(updateSingleOrder));
            results.push(...chunkResults);
            if (orders.length > concurrency) await new Promise(resolve => setTimeout(resolve, 500));
        }
        
        // 统计结果（保持不变）
        const totalSuccess = results.filter(r => r.status === 'fulfilled' && r.value.success).length;
        const totalSkipped = results.filter(r => r.status === 'fulfilled' && r.value.skipped).length;
        const totalFailed = results.length - totalSuccess - totalSkipped;
        
        // logger.info(`GOFO更新完成：成功=${totalSuccess}，跳过=${totalSkipped}，失败=${totalFailed}`);
        return { 
            success: true, total: results.length, success: totalSuccess, 
            skipped: totalSkipped, failed: totalFailed, logistics: 'GOFO' 
        };
        
    } catch (error) {
        logger.error(`GOFO更新任务失败：${error.message}`);
        return { success: false, error: error.message, logistics: 'GOFO' };
    } finally {
        conn.release();
    }
};


// 时间格式化函数（保持修正后的逻辑）
function formatGofoDateTime(dateTimeStr) {
    if (!dateTimeStr) return null;
    
    try {
        const date = new Date(dateTimeStr);
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const hours = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');
        const seconds = String(date.getSeconds()).padStart(2, '0');
        
        return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    } catch (error) {
        logger.error(`GOFO时间格式化失败: ${dateTimeStr}, 错误: ${error.message}`);
        return null;
    }
}
// UPS物流更新函数
const concurrentUpdateUpsLogistics = async (concurrency = 1000, filteredIds = []) => {
    // logger.info(`开始UPS物流更新，并发数: ${concurrency}，筛选ID: ${filteredIds.length > 0 ? filteredIds.join(',') : '无'}`);
    const conn = await pool.getConnection();
    try {
        const baseSql = `
            SELECT id, 跟踪单号, 物流状态 AS originalStatus 
            FROM wuliu_data 
            WHERE 物流商 = 'ups' 
            AND (物流状态 IS NULL OR 物流状态 != '已签收')
        `;
        
        const sql = filteredIds.length > 0 ? `${baseSql} AND id IN (?)` : baseSql;
        const queryParams = filteredIds.length > 0 ? [filteredIds] : [];
        
        const [orders] = await conn.query(sql, queryParams);
        // logger.info(`找到 ${orders.length} 条需要更新的UPS订单`);
        if (orders.length === 0) return { success: true, count: 0, logistics: 'UPS' };
        
        const updateSingleOrder = async (order) => {
            try {
                const { id, 跟踪单号: trackingNumber, originalStatus } = order;
                // UPS使用与FedEx相同的接口，POST请求包含跟踪单号
                const response = await axios.post(
                    'https://wdbso.vip/get_wuliu',
                    { trackingNumbers: [trackingNumber] },
                    {
                        headers: { 
                            'token': '3d71e96301854e1975b8a714059e9bb3712a0bb6a0bfd3ab652835b0e12f00ea', 
                            'Content-Type': 'application/json'
                        },
                        timeout: 30000
                    }
                );
                
                // 验证接口返回格式
                if (!response.data.success || !response.data.data || !response.data.data.data) {
                    throw new Error(`UPS接口返回异常`);
                }
                
                const data = response.data.data.data;
                const upsData = data.find(item => item.trackingNumber === trackingNumber);
                if (!upsData) throw new Error(`未找到跟踪号 ${trackingNumber} 的有效数据`);
                
                const tracks = upsData.trackDetailDTOS || [];
                if (tracks.length === 0) throw new Error(`无轨迹数据`);
                
                // 状态映射关系（基于UPS接口返回的statusMessage）
                const statusMap = {
                    '已递送': '已签收',
                    '准备今天递送': '派送中',
                    'UPS 设施处理': '运输中',
                    '抵达设施': '运输中',
                    '离开设施': '运输中',
                    '发件人已创建标签，但是 UPS 尚未收到包裹。': '订单创建'
                };
                
                // 格式化时间（提取YYYY-MM-DD HH:MM:SS）
                const formatDateTime = (dateTimeStr) => {
                    if (!dateTimeStr) return null;
                    const match = dateTimeStr.match(/^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/);
                    return match ? match[1] : null;
                };
                
                // 最新轨迹（取第一条，接口返回的轨迹按时间倒序排列）
                const latestTrack = tracks[0];
                const statusMessage = latestTrack.statusMessage || '';
                // 从状态消息中提取关键状态
                const logisticsStatus = statusMap[statusMessage] || '等待同步';
                
                // 提取妥投时间（状态为已签收时）
                let signTime = null;
                if (logisticsStatus === '已签收') {
                    // 查找状态为"已递送"的轨迹
                    const deliveredTrack = tracks.find(track => track.statusMessage.includes('已递送'));
                    if (deliveredTrack) {
                        signTime = formatDateTime(deliveredTrack.startTime);
                    } else {
                        logger.warn(`UPS标记为"已签收"，但未找到"已递送"轨迹：${trackingNumber}`);
                    }
                }
                
                // 保留原数据逻辑（新状态未知且原状态有效时）
                const isNewStatusUnknown = logisticsStatus === '等待同步';
                const isOriginalStatusValid = originalStatus && 
                                            originalStatus !== '等待同步' && 
                                            originalStatus !== '未知';
                
                if (isNewStatusUnknown && isOriginalStatusValid) {
                    // logger.info(`UPS保留原数据：${trackingNumber}（原状态：${originalStatus}）`);
                    return { success: true, trackingNumber, skipped: true, logistics: 'UPS' };
                }
                
                // 提取上网时间（第一条运输中状态的时间）
                let onlineTime = null;
                const transitKeywords = ['UPS 设施处理', '抵达设施', '离开设施'];
                // 反转轨迹数组，按时间正序查找第一条运输中状态
                const sortedTracks = [...tracks].reverse();
                for (const track of sortedTracks) {
                    if (transitKeywords.some(keyword => track.statusMessage.includes(keyword))) {
                        onlineTime = formatDateTime(track.startTime);
                        break;
                    }
                }
                
                // 最新轨迹详情和时间
                const latestTrackDetail = statusMessage || null;
                const latestTrackTime = formatDateTime(latestTrack.startTime);
                
                // 更新数据库
                const updateSql = `
                    UPDATE wuliu_data 
                    SET 上网时间 = ?, 最新轨迹详情 = ?, 最新轨迹时间 = ?, 物流状态 = ?,
                        妥投时间 = ?
                    WHERE id = ?
                `;
                await conn.query(updateSql, [
                    onlineTime,
                    latestTrackDetail,
                    latestTrackTime,
                    logisticsStatus,
                    signTime,
                    id
                ]);
                
                // logger.info(`UPS更新成功：${trackingNumber} → ${logisticsStatus}（妥投时间：${signTime || '无'}）`);
                return { success: true, trackingNumber, logistics: 'UPS' };
                
            } catch (error) {
                logger.error(`UPS更新失败：${order.跟踪单号}，错误：${error.message}`);
                try {
                    // 失败时重置状态
                    await conn.query(`UPDATE wuliu_data SET 物流状态 = '等待同步', 妥投时间 = null WHERE id = ?`, [order.id]);
                } catch (dbError) {
                    logger.error(`设置等待同步失败：${dbError.message}`);
                }
                return { success: false, trackingNumber: order.跟踪单号, error: error.message, logistics: 'UPS' };
            }
        };
        
        // 分块并发处理
        const processChunk = async (chunk) => {
            const results = await Promise.allSettled(chunk.map(updateSingleOrder));
            const successes = results.filter(r => r.status === 'fulfilled' && r.value.success).length;
            // logger.info(`UPS批次完成：成功=${successes}，失败=${chunk.length - successes}`);
            return results;
        };
        
        const chunks = [];
        for (let i = 0; i < orders.length; i += concurrency) {
            chunks.push(orders.slice(i, i + concurrency));
        }
        
        const allResults = [];
        for (const chunk of chunks) {
            allResults.push(...await processChunk(chunk));
            if (orders.length > concurrency) await new Promise(resolve => setTimeout(resolve, 500));
        }
        
        // 统计结果
        const totalSuccess = allResults.filter(r => r.status === 'fulfilled' && r.value.success).length;
        const totalSkipped = allResults.filter(r => r.status === 'fulfilled' && r.value.skipped).length;
        const totalFailed = allResults.length - totalSuccess - totalSkipped;
        
        // logger.info(`UPS更新完成：成功=${totalSuccess}，跳过=${totalSkipped}，失败=${totalFailed}`);
        return { 
            success: true, 
            total: allResults.length, 
            success: totalSuccess, 
            skipped: totalSkipped, 
            failed: totalFailed,
            logistics: 'UPS'
        };
        
    } catch (error) {
        logger.error(`UPS更新任务失败：${error.message}`);
        return { success: false, error: error.message, logistics: 'UPS' };
    } finally {
        conn.release();
    }
};


// Amazon物流更新函数
const concurrentUpdateAmazonLogistics = async (concurrency = 1000, filteredIds = []) => {
    logger.info(`开始Amazon物流更新，并发数: ${concurrency}，筛选ID: ${filteredIds.length > 0 ? filteredIds.join(',') : '无'}`);
    const conn = await pool.getConnection();
    try {
        const baseSql = `
            SELECT id, 跟踪单号, 物流状态 AS originalStatus 
            FROM wuliu_data 
            WHERE 渠道 LIKE 'amazon' 
            AND (物流状态 IS NULL OR 物流状态 != '已签收')
        `;
        
        const sql = filteredIds.length > 0 ? `${baseSql} AND id IN (?)` : baseSql;
        const queryParams = filteredIds.length > 0 ? [filteredIds] : [];
        
        const [orders] = await conn.query(sql, queryParams);
        logger.info(`找到 ${orders.length} 条需要更新的Amazon订单`);
        if (orders.length === 0) return { success: true, count: 0, logistics: 'AMAZON' };
        
        const updateSingleOrder = async (order) => {
            try {
                const { id, 跟踪单号: trackingNumber, originalStatus } = order;
                // 调用Amazon接口（GET请求）
                const response = await axios.get(
                    `https://wdbso.vip/get_amazon?id=${trackingNumber}`,
                    {
                        headers: { 
                            'token': '3d71e96301854e1975b8a714059e9bb3712a0bb6a0bfd3ab652835b0e12f00ea'
                        },
                        timeout: 30000
                    }
                );
                
                // 验证接口返回
                if (!response.data.success || !response.data.data) {
                    throw new Error(`Amazon接口返回异常`);
                }
                
                const apiData = response.data.data;
                const processedData = processAmazonData(apiData);
                
                // 映射为系统内部状态
                const statusMap = {
                    '已创建': '订单创建',
                    '已取件': '运输中',
                    '已到达分拣中心': '运输中',
                    '已离开分拣中心': '运输中',
                    '已接收': '运输中',
                    '已发出': '运输中',
                    '配送中': '派送中',
                    '派送中': '派送中',
                    '已送达': '已签收'
                };
                const logisticsStatus = statusMap[processedData.status] || '等待同步';
                
                // 提取妥投时间（状态为已签收时）
                let signTime = null;
                if (logisticsStatus === '已签收') {
                    const deliveredEvent = processedData.events.find(event => 
                        event.status === '已送达'
                    );
                    if (deliveredEvent) {
                        signTime = `${deliveredEvent.date} ${deliveredEvent.time}`;
                    } else {
                        logger.warn(`Amazon标记为"已签收"，但未找到对应事件：${trackingNumber}`);
                    }
                }
                
                // 保留原数据逻辑（新状态未知且原状态有效时）
                const isNewStatusUnknown = logisticsStatus === '等待同步';
                const isOriginalStatusValid = originalStatus && 
                                            originalStatus !== '等待同步' && 
                                            originalStatus !== '未知';
                
                if (isNewStatusUnknown && isOriginalStatusValid) {
                    logger.info(`Amazon保留原数据：${trackingNumber}（原状态：${originalStatus}）`);
                    return { success: true, trackingNumber, skipped: true, logistics: 'AMAZON' };
                }
                
                // 提取上网时间（第一条运输中状态的时间）
                let onlineTime = null;
                const transitStatus = ['已取件', '已到达分拣中心', '已离开分拣中心', '已接收', '已发出'];
                // 查找第一条符合运输中状态的事件
                for (const event of processedData.events) {
                    if (transitStatus.includes(event.status)) {
                        onlineTime = `${event.date} ${event.time}`;
                        break;
                    }
                }
                
                // 最新轨迹详情和时间
                const latestEvent = processedData.events[processedData.events.length - 1] || {};
                const latestTrackDetail = latestEvent.status || null;
                const latestTrackTime = latestEvent.date && latestEvent.time 
                    ? `${latestEvent.date} ${latestEvent.time}` 
                    : null;
                
                // 更新数据库
                const updateSql = `
                    UPDATE wuliu_data 
                    SET 上网时间 = ?, 最新轨迹详情 = ?, 最新轨迹时间 = ?, 物流状态 = ?,
                        妥投时间 = ?
                    WHERE id = ?
                `;
                await conn.query(updateSql, [
                    onlineTime,
                    latestTrackDetail,
                    latestTrackTime,
                    logisticsStatus,
                                        signTime,
                    id
                ]);
                
                logger.info(`Amazon更新成功：${trackingNumber} → ${logisticsStatus}（妥投时间：${signTime || '无'}）`);
                return { success: true, trackingNumber, logistics: 'AMAZON' };
                
            } catch (error) {
                logger.error(`Amazon更新失败：${order.跟踪单号}，错误：${error.message}`);
                try {
                    // 失败时重置状态
                    await conn.query(`UPDATE wuliu_data SET 物流状态 = '等待同步', 妥投时间 = null WHERE id = ?`, [order.id]);
                } catch (dbError) {
                    logger.error(`设置等待同步失败：${dbError.message}`);
                }
                return { success: false, trackingNumber: order.跟踪单号, error: error.message, logistics: 'AMAZON' };
            }
        };
        
        // 分块并发处理
        const processChunk = async (chunk) => {
            const results = await Promise.allSettled(chunk.map(updateSingleOrder));
            const successes = results.filter(r => r.status === 'fulfilled' && r.value.success).length;
            logger.info(`Amazon批次完成：成功=${successes}，失败=${chunk.length - successes}`);
            return results;
        };
        
        const chunks = [];
        for (let i = 0; i < orders.length; i += concurrency) {
            chunks.push(orders.slice(i, i + concurrency));
        }
        
        const allResults = [];
        for (const chunk of chunks) {
            allResults.push(...await processChunk(chunk));
            if (orders.length > concurrency) await new Promise(resolve => setTimeout(resolve, 500));
        }
        
        // 统计结果
        const totalSuccess = allResults.filter(r => r.status === 'fulfilled' && r.value.success).length;
        const totalSkipped = allResults.filter(r => r.status === 'fulfilled' && r.value.skipped).length;
        const totalFailed = allResults.length - totalSuccess - totalSkipped;
        
        logger.info(`Amazon更新完成：成功=${totalSuccess}，跳过=${totalSkipped}，失败=${totalFailed}`);
        return { 
            success: true, 
            total: allResults.length, 
            success: totalSuccess, 
            skipped: totalSkipped, 
            failed: totalFailed,
            logistics: 'AMAZON'
        };
        
    } catch (error) {
        logger.error(`Amazon更新任务失败：${error.message}`);
        return { success: false, error: error.message, logistics: 'AMAZON' };
    } finally {
        conn.release();
    }
};


// 处理Amazon数据（基于提供的解析逻辑）
function processAmazonData(apiData) {
    let resultData = {
        status: '未知状态',
        estimatedDeliveryDate: '未提供',
        serviceType: 'Amazon Logistics',
        originLocation: '未知',
        destinationLocation: '未知',
        lastUpdated: '未提供',
        events: []
    };

    // 强制解析JSON字符串（关键步骤）
    let progressTracker = {};
    let eventHistory = { eventHistory: [] };
    try {
        progressTracker = JSON.parse(apiData.progressTracker || '{}');
        eventHistory = JSON.parse(apiData.eventHistory || '{"eventHistory":[]}');
    } catch (e) {
        logger.error('解析Amazon数据失败:', e);
        return resultData;
    }

    // 提取状态和时间
    if (progressTracker.summary) {
        resultData.status = formatAmazonStatus(progressTracker.summary.status);
        resultData.estimatedDeliveryDate = formatDateTime(progressTracker.summary.metadata?.expectedDeliveryDate?.date);
    }

    // 提取事件并处理空数据
    resultData.events = formatAmazonEvents(eventHistory.eventHistory);
    
    // 补充发货地和目的地（从第一个和最后一个非空地址提取）
    const validEvents = resultData.events.filter(e => e.location !== '未知地点');
    if (validEvents.length > 0) {
        resultData.originLocation = validEvents[0].location;
        resultData.destinationLocation = validEvents[validEvents.length - 1].location;
    }

    // 提取最后更新时间
    if (resultData.events.length > 0) {
        resultData.lastUpdated = `${resultData.events[resultData.events.length - 1].date} ${resultData.events[resultData.events.length - 1].time}`;
    }

    return resultData;
}

// 格式化Amazon事件（去重并保留带地址的记录）
function formatAmazonEvents(events) {
    if (!events || events.length === 0) return [];

    // 用于临时存储去重后的事件（键：事件标识+时间戳，值：事件对象）
    const uniqueEvents = new Map();

    events.forEach(event => {
        // 1. 生成事件唯一标识（基于事件类型和时间戳，精确到分钟，避免过度去重）
        const eventKey = `${event.eventCode}-${new Date(event.eventTime).getTime() / 60000 | 0}`;
        
        // 2. 判断当前事件是否包含地址信息
        const hasLocation = !!event.location?.city;

        // 3. 去重逻辑：
        // - 如果是新事件（无重复），直接添加
        // - 如果是重复事件，保留包含地址的版本
        if (!uniqueEvents.has(eventKey)) {
            // 新事件，直接存入
            uniqueEvents.set(eventKey, event);
        } else {
            // 重复事件，比较地址信息：保留有地址的版本
            const existingEvent = uniqueEvents.get(eventKey);
            const existingHasLocation = !!existingEvent.location?.city;
            
            if (hasLocation && !existingHasLocation) {
                // 替换为包含地址的事件
                uniqueEvents.set(eventKey, event);
            }
        }
    });

    // 4. 处理去重后的事件，格式化显示信息
    return Array.from(uniqueEvents.values()).map(event => {
        const locationParts = [];
        if (event.location?.city) locationParts.push(event.location.city);
        if (event.location?.stateProvince) locationParts.push(event.location.stateProvince);
        if (event.location?.countryCode) locationParts.push(formatCountryCode(event.location.countryCode));
        
        // 优化空地址的显示文本
        let location;
        if (locationParts.length > 0) {
            location = locationParts.join(', ');
        } else {
            if (event.eventCode === 'CreationConfirmed') {
                location = '订单创建（无具体位置）';
            } else if (event.eventCode === 'Delivered') {
                location = '送达地址（隐私保护）';
            } else {
                location = '合作商运送中';
            }
        }

        // 格式化时间为：年-月-日 时:分:秒
        const dateTime = new Date(event.eventTime);
        const date = `${dateTime.getFullYear()}-${String(dateTime.getMonth() + 1).padStart(2, '0')}-${String(dateTime.getDate()).padStart(2, '0')}`;
        const time = `${String(dateTime.getHours()).padStart(2, '0')}:${String(dateTime.getMinutes()).padStart(2, '0')}:${String(dateTime.getSeconds()).padStart(2, '0')}`;

        return {
            date: date,
            time: time,
            gmtOffset: '+00:00',
            status: formatAmazonStatus(event.statusSummary?.localisedStringId || event.eventCode),
            location: location,
            // 保留时间戳用于排序
            timestamp: dateTime.getTime()
        };
    }).sort((a, b) => a.timestamp - b.timestamp); // 按时间正序排列（ oldest first）
}

// 辅助函数：格式化Amazon状态
function formatAmazonStatus(statusCode) {
    if (!statusCode) return '未知状态';

    const statusMap = {
        // 原有映射
        'package_out_for_delivery': '包裹正在派送中',
        'package_delivered': '包裹已送达',
        // 新增事件编码映射
        'swa_rex_detail_creation_confirmed': '已创建',
        'swa_rex_detail_pickedUp': '已取件',
        'swa_rex_arrived_at_sort_center': '已到达分拣中心',
        'swa_rex_detail_departed': '已离开分拣中心',
        'swa_rex_detail_arrived_at_delivery_Center': '配送中',
        'swa_rex_detail_delivered': '已送达',
        'CreationConfirmed': '已创建',
        'PickupDone': '已取件',
        'Received': '已接收',
        'Departed': '已发出',
        'OutForDelivery': '派送中',
        'Delivered': '已送达',
        // 从progressTracker提取的状态
        'Delivered': '已送达',
        'Out for Delivery': '派送中',
        'In Transit': '运输中',
        'Shipping Label Created': '已创建'
    };

    return statusMap[statusCode] || statusCode;
}

// 辅助函数：格式化日期
function formatDateTime(dateStr) {
    if (!dateStr) return '未提供';
    try {
        const date = new Date(dateStr);
        return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')} ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}:${String(date.getSeconds()).padStart(2, '0')}`;
    } catch (e) {
        return dateStr;
    }
}

// 辅助函数：格式化国家代码
function formatCountryCode(code) {
    const countryMap = {
        'US': '美国',
        'CN': '中国',
        'GB': '英国',
        'CA': '加拿大',
        'JP': '日本'
    };
    return countryMap[code] || code;
}


// 更新全量物流更新函数，包含Amazon
const updateAllLogistics = async (concurrency = 1000) => {
    logger.info(`开始全量物流更新（USPS+UNIUNI+FedEx+GOFO+UPS+Amazon），并发数: ${concurrency}`);
    try {
        const results = [];
        // 依次执行各物流商更新（不传递filteredIds，即更新全部符合条件数据）
        results.push(await concurrentUpdateUspsLogistics(concurrency));
        results.push(await concurrentUpdateUniuniLogistics(concurrency));
        results.push(await concurrentUpdateFedExLogistics(concurrency));
        results.push(await concurrentUpdateGofoLogistics(concurrency));
        results.push(await concurrentUpdateUpsLogistics(concurrency));
        results.push(await concurrentUpdateAmazonLogistics(concurrency)); // 新增Amazon
        
        // 汇总结果
        const total = results.reduce((sum, r) => sum + (r.total || 0), 0);
        const success = results.reduce((sum, r) => sum + (r.success || 0), 0);
        const skipped = results.reduce((sum, r) => sum + (r.skipped || 0), 0);
        const failed = results.reduce((sum, r) => sum + (r.failed || 0), 0);
        
        logger.info(`全量物流更新完成：总订单=${total}，成功=${success}，跳过=${skipped}，失败=${failed}`);
        return { 
            success: true,
            details: results,
            total,
            success,
            skipped,
            failed
        };
    } catch (error) {
        logger.error(`全量物流更新失败：${error.message}`);
        return { success: false, error: error.message };
    }
};

// 更新HTTP接口描述，包含Amazon
if (typeof app !== 'undefined' && app.get) {
    app.get('/upwuliu', async (req, res) => {
        try {
            const concurrency = parseInt(req.query.concurrency) || 1000;
            const result = await updateAllLogistics(concurrency);
            res.json(result);
        } catch (error) {
            logger.error(`/upwuliu接口失败：${error.message}`);
            res.status(500).json({ success: false, error: error.message });
        }
    });
    logger.info('已注册物流更新接口：/upwuliu（含USPS+UNIUNI+FedEx+GOFO+UPS+Amazon）');
}

// 优化：按条件筛选更新物流的接口
if (typeof app !== 'undefined' && app.post) {
    app.post('/upwuliuu', async (req, res) => {
        const { conditions, concurrency = 1000, logisticsTypes = [], batchSize = 500 } = req.body;
        logger.info(`收到按条件更新物流请求，条件: ${JSON.stringify(conditions)}, 并发数: ${concurrency}, 物流商: ${logisticsTypes.join(',') || '全部'}, 批次大小: ${batchSize}`);

        // 校验参数
        if (!conditions || Object.keys(conditions).length === 0) {
            return res.status(400).json({ success: false, error: '请至少指定一个筛选条件' });
        }

        // 允许的筛选字段（防止SQL注入和无效字段）
        const allowedFields = [
            'id', '客户名称', '跟踪单号', '渠道', '渠道组', 
            '出库时间', '上网时间', '物流商', '物流状态', 
            '仓库'
        ];

        // 验证条件字段合法性
        const invalidFields = Object.keys(conditions).filter(field => !allowedFields.includes(field));
        if (invalidFields.length > 0) {
            return res.status(400).json({ 
                success: false, 
                error: `不支持的筛选字段: ${invalidFields.join(',')}，允许的字段: ${allowedFields.join(',')}` 
            });
        }

        const conn = await pool.getConnection();
        try {
            // 构建查询条件（参数化查询，防止SQL注入）
            let whereClause = [];
            let queryParams = [];
            Object.entries(conditions).forEach(([field, value]) => {
                // 优化：对可精确匹配的字段使用=而非LIKE
                if (['渠道', '渠道组', '物流商', '物流状态', '仓库'].includes(field)) {
                    whereClause.push(`${field} = ?`);
                    queryParams.push(value);
                } 
                // 保留必要的模糊匹配
                else if (['客户名称', '跟踪单号'].includes(field)) {
                    whereClause.push(`${field} LIKE ?`);
                    queryParams.push(`%${value}%`);
                } 
                // 处理日期类型
                else if (['出库时间', '上网时间'].includes(field)) {
                    whereClause.push(`${field} = ?`);
                    queryParams.push(value);
                } 
                // 处理ID
                else if (field === 'id') {
                    if (Array.isArray(value)) {
                        whereClause.push(`${field} IN (${value.map(() => '?').join(',')})`);
                        queryParams.push(...value);
                    } else {
                        whereClause.push(`${field} = ?`);
                        queryParams.push(value);
                    }
                }
            });

            // 基础条件：只更新未签收的订单
            whereClause.push(`(物流状态 IS NULL OR 物流状态 != '已签收')`);

            // 拼接查询SQL
            const baseQuerySql = `
                SELECT id, 跟踪单号, 物流状态 AS originalStatus, 渠道 
                FROM wuliu_data 
                WHERE ${whereClause.join(' AND ')}
            `;

            // 执行查询，获取符合条件的订单
            logger.info(`执行筛选查询: ${baseQuerySql}, 参数: ${JSON.stringify(queryParams)}`);
            const [orders] = await conn.query(baseQuerySql, queryParams);
            logger.info(`找到符合条件的订单: ${orders.length} 条`);

            if (orders.length === 0) {
                return res.json({ success: true, message: '未找到符合条件的订单', total: 0 });
            }

            // 按物流商筛选（如果指定了logisticsTypes）
            const filteredOrders = logisticsTypes.length > 0 
                ? orders.filter(order => logisticsTypes.some(type => order.渠道.includes(type.toUpperCase())))
                : orders;

            if (filteredOrders.length === 0) {
                return res.json({ success: true, message: '未找到符合条件的指定物流商订单', total: 0 });
            }

            // 支持的物流商更新函数映射
            const logisticsFunctions = {
                'USPS': concurrentUpdateUspsLogistics,
                'UNIUNI': concurrentUpdateUniuniLogistics,
                'FEDEX': concurrentUpdateFedExLogistics,
                'GOFO': concurrentUpdateGofoLogistics,
                'UPS': concurrentUpdateUpsLogistics,
                'AMAZON': concurrentUpdateAmazonLogistics
            };

            // 执行指定的物流商更新（未指定则更新全部）
            const targetFunctions = logisticsTypes.length > 0
                ? logisticsTypes.map(type => logisticsFunctions[type.toUpperCase()]).filter(Boolean)
                : Object.values(logisticsFunctions);

            // 分批处理订单ID
            const total = filteredOrders.length;
            const allResults = [];
            
            // 计算批次
            const batches = Math.ceil(total / batchSize);
            
            for (let batch = 0; batch < batches; batch++) {
                const startIdx = batch * batchSize;
                const endIdx = Math.min((batch + 1) * batchSize, total);
                const batchOrderIds = filteredOrders.slice(startIdx, endIdx).map(order => order.id);
                
                logger.info(`处理批次 ${batch + 1}/${batches}，订单数: ${batchOrderIds.length}`);
                
                // 并行处理不同物流商
                const batchResults = await Promise.all(
                    targetFunctions.map(async updateFunc => {
                        try {
                            return await updateFunc.call(null, concurrency, batchOrderIds);
                        } catch (error) {
                            logger.error(`物流商更新失败: ${updateFunc.name}，错误: ${error.message}`);
                            return { success: false, error: error.message, total: batchOrderIds.length, success: 0, skipped: 0, failed: batchOrderIds.length };
                        }
                    })
                );
                
                // 汇总批次结果
                allResults.push(...batchResults);
                
                // 简单进度报告
                if (batches > 1) {
                    const currentSuccess = allResults.reduce((sum, r) => sum + (r.success || 0), 0);
                    const currentSkipped = allResults.reduce((sum, r) => sum + (r.skipped || 0), 0);
                    const currentFailed = allResults.reduce((sum, r) => sum + (r.failed || 0), 0);
                    logger.info(`批次 ${batch + 1}/${batches} 完成，当前总结果: 成功=${currentSuccess}，跳过=${currentSkipped}，失败=${currentFailed}`);
                }
            }

            // 汇总最终结果
            const finalSuccess = allResults.reduce((sum, r) => sum + (r.success || 0), 0);
            const finalSkipped = allResults.reduce((sum, r) => sum + (r.skipped || 0), 0);
            const finalFailed = allResults.reduce((sum, r) => sum + (r.failed || 0), 0);

            res.json({
                success: true,
                total,
                success: finalSuccess,
                skipped: finalSkipped,
                failed: finalFailed,
                details: allResults
            });

        } catch (error) {
            logger.error(`按条件更新物流接口失败: ${error.message}`, error.stack);
            res.status(500).json({ success: false, error: error.message });
        } finally {
            if (conn) conn.release();
        }
    });

    logger.info('已注册优化后的按条件更新物流接口：POST /upwuliuu');
}

// 更新定时任务描述，包含Amazon
cron.schedule('0 */4 * * *', async () => {
    logger.info('开始执行每4小时物流更新定时任务（含Amazon）');
    try {
        await fetchSkuKcData(); // 原库存逻辑
        await updateAllLogistics(); // 全量更新（含Amazon）
        logger.info('每4小时定时任务执行完成');
    } catch (error) {
        logger.error(`定时任务执行失败：${error.stack}`);
    }
}, { timezone: 'Asia/Shanghai' });

logger.info('已启动每4小时库存+物流（含Amazon）更新定时任务');