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
        sb.append(output.length());
        sb.append("\r\n");
        sb.append(output);
        sb.append("\r\n");
        return sb.toString();
    }
}
