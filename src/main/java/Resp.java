import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

public class Resp {
    static String[] decodeBulkString(BufferedReader reader, int length) throws IOException {
        if (length == -1) return null;
        String[] decoded = new String[length];
        for (int i = 0; i < length; i++) {
            String lineLen = reader.readLine();
            String line = reader.readLine();
            decoded[i] = line;
        }
        return decoded;
    }
    static String encodeBulkString(String output)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("$");
        if(output!=null) {
            sb.append(output.length());
            sb.append("\r\n");
            sb.append(output);
        }
        else sb.append(-1);
        sb.append("\r\n");
        return sb.toString();
    }
    static String encodeInteger(int num)
    {
        if(num==0)
            return ":0\r\n";

        StringBuilder sb = new StringBuilder();
        sb.append(":");
        char sign='+';
        if(num<0)
        {
            sign='-';
            num*=-1;
        }
        sb.append(sign);
        while(num>0)
        {
            sb.append(num%10);
            num/=10;
        }
        sb.append("\r\n");
        return sb.toString();
    }
    static String encodeArray(List<String> list)
    {
        if(list==null)
            return "*-1\r\n";
        StringBuilder sb = new StringBuilder();
        sb.append('*');
        sb.append(list.size());
        sb.append("\r\n");
        for(String s: list)
        {
            sb.append("$");
            sb.append(s.length());
            sb.append("\r\n");
            sb.append(s);
            sb.append("\r\n");
        }
        return sb.toString();
    }

    static String encodeSimpleString(String s)
    {
        return "+"+s+"\r\n";
    }

    static StreamId parseStreamId(String s)
    {
        String[] parts = s.split("-");
        long time = Long.parseLong(parts[0]);
        long seq = Long.parseLong(parts[1]);
        return new StreamId(time, seq);
    }

    static String encodeError(String err)
    {
        return "-ERR "+err+"\r\n";
    }
}
