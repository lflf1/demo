const mysql = require('mysql2/promise');
const moment = require('moment-timezone');
const config = require('./config');
const logger = require('./logger');

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

// 将中国时间转换为美国时间
function convertToUSATime(chinaTime) {
    return moment(chinaTime).tz('America/New_York').toDate();
}

// 检查是否为休息日（周六和周日）
function isWeekend(date) {
    const day = moment(date).day();
    return day === 0 || day === 6;
}

// 美国节假日列表（简化示例，可按需补充）
const usHolidays = [
    // 元旦
    moment('2025-01-01').toDate(),
    // 马丁·路德·金纪念日（第三个星期一）
    moment('2025-01-20').toDate(),
    // 总统日（第三个星期一）
    moment('2025-02-17').toDate(),
    // 阵亡将士纪念日（最后一个星期一）
    moment('2025-05-26').toDate(),
    // 独立日
    moment('2025-07-04').toDate(),
    // 劳动节（第一个星期一）
    moment('2025-09-01').toDate(),
    // 哥伦布日（第二个星期一）
    moment('2025-10-13').toDate(),
    // 退伍军人节
    moment('2025-11-11').toDate(),
    // 感恩节（第四个星期四）
    moment('2025-11-28').toDate(),
    // 圣诞节
    moment('2025-12-25').toDate()
];

// 检查是否为美国节假日
function isUSHoliday(date) {
    const year = moment(date).year();
    const month = moment(date).month();
    const day = moment(date).date();
    return usHolidays.some(holiday => {
        return moment(holiday).year() === year && moment(holiday).month() === month && moment(holiday).date() === day;
    });
}

// 计算两个时间之间的差值（以小时为单位），去掉休息日和美国节假日
function calculateTimeDifference(start, end) {
    if (!start || !end) return 888;
    let current = moment(start);
    const endDate = moment(end);
    let totalHours = 0;

    while (current.isSameOrBefore(endDate, 'day')) {
        if (!isWeekend(current) && !isUSHoliday(current)) {
            const startOfDay = current.clone().startOf('day');
            const endOfDay = current.clone().endOf('day');
            const effectiveStart = moment.max(startOfDay, moment(start));
            const effectiveEnd = moment.min(endOfDay, moment(end));
            totalHours += effectiveEnd.diff(effectiveStart, 'hours');
        }
        current.add(1, 'day');
    }
    return totalHours;
}

// 封装计算时效并转换为天的函数
function calculateEfficiencyInDays(start, end) {
    const efficiency = calculateTimeDifference(start, end);
    return efficiency === 888 ? 888 : efficiency / 24;
}

// 修改 calculateAndUpdateTimeEfficiency 函数，使其更新整个 ck_data 表的数据
async function calculateAndUpdateTimeEfficiency() {
    const conn = await pool.getConnection();
    try {
        const [rows] = await conn.query('SELECT * FROM ck_data');
        for (const row of rows) {
            const outboundTimeUSA = convertToUSATime(row.OutboundTime);
            const creationTimeUSA = convertToUSATime(row.Creation_time);

            // 计算出库时效
            const outboundEfficiencyInDays = calculateEfficiencyInDays(creationTimeUSA, outboundTimeUSA);
            // 计算上网时效
            const onlineEfficiencyInDays = calculateEfficiencyInDays(outboundTimeUSA, moment(row.Receipt_Time).toDate());
            // 计算妥投时效
            const deliveryEfficiencyInDays = calculateEfficiencyInDays(outboundTimeUSA, moment(row.Delivered_Time).toDate());

            await conn.query(
                'UPDATE ck_data SET 出库时效 =?, 上网时效 =?, 妥投时效 =? WHERE id =?',
                [outboundEfficiencyInDays, onlineEfficiencyInDays, deliveryEfficiencyInDays, row.id]
            );
        }
        logger.info('时效信息已计算并更新到数据库');
    } catch (error) {
        logger.error(`计算和更新时效信息失败: ${error.stack}`);
    } finally {
        conn.release();
    }
}

// 执行刷新操作
async function refreshTimeEfficiency() {
    try {
        await calculateAndUpdateTimeEfficiency();
        console.log('ck_data 表的时效信息刷新完成');
    } catch (error) {
        console.error('刷新时效信息时出错:', error);
    }
}

refreshTimeEfficiency();    