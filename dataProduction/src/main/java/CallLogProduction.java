import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 这是一个用来产生通话日志的类
 */
public class CallLogProduction {
    //用两个容器存放通讯录的姓名和号码
    private List<String> nameList = null;
    private List<String> phoneList = null;
    Random random = new Random();

    public CallLogProduction(){
        this.getNameAndPhone();
    }

    public static void main(String[] args) {
        CallLogProduction createLogs = new CallLogProduction();
        List<String> logs = new ArrayList<String>();
        //产生10000条通话数据
        long start = System.currentTimeMillis();
        for (int i = 0;i<20000000;i++) {
            String date_time = createLogs.createDate_Time();
            int drution = createLogs.createDrution();
            int flag = createLogs.createFlag();
            String nameAndPhone = createLogs.randomNameAndPhone();
            String log = date_time + " " + nameAndPhone + " " + drution + " " + flag;
            logs.add(log);
           if(logs.size()==200000) {
               createLogs.writeLogs(logs);
               logs.clear();//对于数据量比大的情况下，当log记录数达到一定的量便开启写操作，对于IO性能提升非常巨大
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(end-start);
    }

    //随机产生通话发生时间，格式：YYYY-MM-DD HH:mm:ss
    private String createDate_Time(){
        int year = random.nextInt(2)+2017;//2017，2018两个年份的数据
        int month = random.nextInt(12)+1;
        int day = 0;//日期需要判断再随机产生
        //28天的月份
        if(month == 2){
            day = random.nextInt(28)+1;
        }
        //31天的月份
        else if((month<=8 && month%2==1) || (month>=8 && month%2==0)){
            day = random.nextInt(31)+1;
        }
        //30天的月份
        else{
            day = random.nextInt(30)+1;
        }

        int hour = random.nextInt(24);
        int minute = random.nextInt(60);
        int second = random.nextInt(60);

        //拼接形成字符串
        String date_time = ""+year+"-";
        if(month<10){
            date_time = date_time+"0"+month+"-";
        }else {
            date_time = date_time+month+"-";
        }
        if(day<10){
            date_time = date_time+"0"+day+" ";
        }else {
            date_time = date_time+day+" ";
        }
        if(hour<10){
            date_time = date_time+"0"+hour+":";
        }else {
            date_time = date_time+hour+":";
        }
        if(minute<10){
            date_time = date_time+"0"+minute+":";
        }else {
            date_time = date_time+minute+":";
        }
        if(second<10){
            date_time = date_time+"0"+second;
        }else {
            date_time = date_time+second;
        }
        return  date_time;
    }

    //随机产生通话时长(单位s) 1到9999s
    private int createDrution(){
        return random.nextInt(9999)+1;//最长10000s
    }

    //标记第一个字段是主呼叫还是被呼叫
    private int createFlag(){
        return random.nextInt(2);
    }

    //随机获取呼叫姓名号码
    private String randomNameAndPhone(){
        int index_call = random.nextInt(nameList.size());//主呼叫
        int index_becall  = random.nextInt(nameList.size());
        while(index_becall == index_call){//主呼叫和被呼叫相同则再次生成
            index_becall = random.nextInt(nameList.size());
        }
        return nameList.get(index_call)+" "+phoneList.get(index_call)+" "+nameList.get(index_becall)+" "+phoneList.get(index_becall);
    }
    //将产生的日志写入到文件中,策略，一次写1000条。
    private void writeLogs(List<String> logs){
        String path = "D:\\hadoop\\data\\callLog\\logs.txt";
        FileWriter writer = null;
        BufferedWriter bw = null;
        try {
            writer = new FileWriter(path,true);
            bw = new BufferedWriter(writer);
            for (String log:logs){
                bw.write(log);
                bw.write("\n");
            }
            bw.flush();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(bw!=null){
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(writer!=null){
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    //提取通讯录中的信息
    private void getNameAndPhone(){
        nameList = new ArrayList<String>();
        phoneList = new ArrayList<String>();
        String path = "D:\\hadoop\\data\\callLog\\通讯录.txt";
        FileReader reader = null;
        BufferedReader br = null;
        try {
            reader = new FileReader(path);
            br = new BufferedReader(reader);
            String line = null;
            while((line = br.readLine())!=null){
                String[] nameAndPhone = line.split("\t");
                nameList.add(nameAndPhone[1]);
                phoneList.add(nameAndPhone[2]);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            //关闭IO流
            if(br!=null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(reader!=null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
