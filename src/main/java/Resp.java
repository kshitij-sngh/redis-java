import javax.imageio.IIOException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Resp {
    static String[] decodeBulk(BufferedReader reader, int length) throws IOException {
        if (length == -1) return null;
        String[] decoded = new String[length];
        for (int i = 0; i < length; i++) {
            String lineLen = reader.readLine();
            String line = reader.readLine();
            decoded[i] = line;
        }
        return decoded;
    }
    static String encodeBulk(String output)
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
            return "+\r\n";

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
}
