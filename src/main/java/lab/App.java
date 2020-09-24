package lab;
import com.google.gson.JsonObject;
import org.json.JSONObject.*;
import java.io.*;
import com.google.gson.*;
import java.lang.System.*;
/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws  Exception
    {

        File file = new File("AA_wiki_00");

        BufferedReader br = new BufferedReader(new FileReader(file));

        String st;
        while ((st = br.readLine()) != null){
            JsonObject obj = new JsonParser().parse(st).getAsJsonObject();
            System.out.println(obj.get("title"));
            //System.out.println(st);

        }

    }
}
