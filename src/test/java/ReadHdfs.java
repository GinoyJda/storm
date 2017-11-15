import java.io.InputStream;

import java.net.URI;
import java.net.URL;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

public class ReadHdfs {
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String args[]) throws Exception {
        ReadFile();
//        ReadUrl();
    }

    public static void ReadFile() throws Exception {
        String uri = "hdfs://10.176.63.105:9000/soc/hadoop/tmp/dfs/data/Hdfs-1-0-1510634654752.txt";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem. get(URI.create (uri), conf);
        InputStream in = null;
        try {
        in = fs.open( new Path(uri));
        IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
        IOUtils.closeStream(in);
        }
     }




    public static void ReadUrl() throws Exception {
        String uri = "hdfs://10.176.63.105:9000/soc/hadoop/tmp/dfs/data";
        InputStream in = null;
        try {
            in = new URL(uri).openStream();
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}