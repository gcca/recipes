import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.ietf.jgss.GSSException;

public class HadoopWithKerberosAuthentication {
  public static void main(String[] args) throws IOException, GSSException {
    // kerberos host and realm
    System.setProperty("java.security.krb5.realm", "GCCA.COM");
    System.setProperty("java.security.krb5.kdc", "192.168.0.1");

    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    conf.set("hadoop.security.authorization", "true");
    conf.set("fs.defaultFS", "hdfs://192.168.0.1");
    conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
    // hack for running locally with fake DNS records
    // true when overriding the host name in /etc/hosts
    conf.set("dfs.client.use.datanode.hostname", "true");

    // server kerberos principal namenode is using
    conf.set("dfs.namenode.kerberos.principal.pattern", "hduser/*@GCCA.COM");

    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab("user@GCCA.COM", "path/to/user.keytab");

    FileSystem fs = FileSystem.get(conf);
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
    while(files.hasNext()) {
      LocatedFileStatus file = files.next();
      System.out.println(IOUtils.toString(fs.open(file.getPath())));
    }
  }
}
