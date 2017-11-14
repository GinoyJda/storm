import java.net.URLDecoder;
import java.util.Date;

import org.productivity.java.syslog4j.Syslog;
import org.productivity.java.syslog4j.SyslogIF;

/**
 * SysLog发送数据
 * @author lish
 */
public class SyslogTest2 {

    public static void main(String[] args){
            try {
                //获取syslog的操作类，使用udp协议。syslog支持"udp", "tcp", "unix_syslog", "unix_socket"协议
                SyslogIF syslog = Syslog.getInstance("udp");
                //设置syslog服务器端地址
                syslog.getConfig().setHost("127.0.0.1");
                //设置syslog接收端口，默认514
                syslog.getConfig().setPort(514);
                //拼接syslog日志，这个日志是自己定义的，通常我们定义成符合公司规范的格式就行，方便查询。例如 操作时间：2014年8月1日  操作者ID:张三 等。信息就是一个字符串。
                StringBuffer buffer = new StringBuffer();
                buffer.append("operatedate：" + new Date().toString().substring(4, 20) + ";");
                buffer.append("operatetimeID:" + "test" + ";");
                buffer.append("operatetime:" + new Date()+ ";");
                buffer.append("type:" + "22"+ ";");
                buffer.append("action:" + "update" + ";");
                buffer.append("common:" + "test syslog");
                /*  发送信息到服务器，2表示日志级别 范围为0~7的数字编码，表示了事件的严重程度。0最高，7最低
                *  syslog为每个事件赋予几个不同的优先级：
                   LOG_EMERG：紧急情况，需要立即通知技术人员。
                   LOG_ALERT：应该被立即改正的问题，如系统数据库被破坏，ISP连接丢失。
                   LOG_CRIT：重要情况，如硬盘错误，备用连接丢失。
                   LOG_ERR：错误，不是非常紧急，在一定时间内修复即可。
                   LOG_WARNING：警告信息，不是错误，比如系统磁盘使用了85%等。
                   LOG_NOTICE：不是错误情况，也不需要立即处理。
                   LOG_INFO：情报信息，正常的系统消息，比如骚扰报告，带宽数据等，不需要处理。
                   LOG_DEBUG：包含详细的开发情报的信息，通常只在调试一个程序时使用。
                */
                System.out.println(buffer.toString());

                    syslog.log(0, URLDecoder.decode(buffer.toString(),"utf-8"));


            } catch (Exception e) {
            }
    }

    public static boolean ipCheck(String text) {
        if (text != null && !text.isEmpty()) {
            // 定义正则表达式
            String regex = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."
                    + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                    + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                    + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$";
            // 判断ip地址是否与正则表达式匹配
            if (text.matches(regex)) {
                return true;
                // 返回判断信息
                //return text + "\n是一个合法的IP地址！";
            } else {
                return false;
                // 返回判断信息
                //return text + "\n不是一个合法的IP地址！";
            }
        }
        return false;
        // 返回判断信息
        //  return "请输入要验证的IP地址！";
    }
}
