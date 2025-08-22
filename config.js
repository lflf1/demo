module.exports = {
    database: {
        host: 'localhost',
        user: 'aoyu',
        password: 'pxCXCMaxiBYPmXGK',
        database: 'aoyu',
        waitForConnections: true,
        connectionLimit: 10,
        charset: 'utf8mb4',
        timezone: '+08:00'  // 强制东八区
    },
    externalApi: {
        url: 'https://omp.xlwms.com/gateway/omp/order/delivery/page',
        requestTemplate: {
            customerCodes: "",  // 改为空值，将在启动时动态填充
            unitMark: 0,
            timeType: "createTime",
            current: 1,
            size: 20,
            total: 0
        },
        headers: {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIlN0IlMjJidXNpbmVzc1R5cGUlMjIlM0ElMjJvbXAlMjIlMkMlMjJsb2dpbkFjY291bnQlMjIlM0ElMjIxODU4MzAwNDcxMyUyMiUyQyUyMnVzZXJOYW1lQ24lMjIlM0ElMjIlRTYlOUQlQTglRTUlQUUlOTclRTQlQkQlOTElMjIlMkMlMjJ1c2VyTmFtZUVuJTIyJTNBJTIyJTIyJTJDJTIyY3VzdG9tZXJDb2RlJTIyJTNBbnVsbCUyQyUyMnRlbmFudENvZGUlMjIlM0ElMjIxNDQwJTIyJTJDJTIydGVybWluYWxUeXBlJTIyJTNBbnVsbCU3RCIsImlzcyI6InhpbmdsaWFuLnNlY3VyaXR5IiwiYnVzaW5lc3NUeXBlIjoib21wIiwiZXhwIjoxNzM5MjQyNzc5LCJpYXQiOjE3MzkxNTYzNzksImp0aSI6IjA4OGQ2YjUzLTA1ZjktNDkyYS05MTNlLWY3MmUzMTQzMjFlNCJ9.-Gzv673JnQWgxd12v3tCIOCGW9iorSOFBCx8rvoN3fc",
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
        }
    },
    server: {
        port: 3000,
        cors: {
            origin: ['http://localhost:3000', 'http://wdbso.vip'],
            methods: ['GET', 'POST']
        }
    }
};
