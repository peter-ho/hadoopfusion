package fusion.hadoop.fusionkeycreation;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.*;
import fusion.hadoop.FusionJobConf;
import fusion.hadoop.*;
import fusion.hadoop.fusionkeycreation.*;

public class FusionKeyCreationMapper extends Mapper<LongWritable, V1, K2, V2>{

  protected Mapper m_inputMapper;
  protected JobConf m_jobConf;
  @Override
  public void Configure(JobConf jobConf) {
	m_jobConf = jobConf;
	m_inputMapper = conf.getMapperClass().createInstance();
	m_inputMapper.Configure();
  }
  
  @Override
  public void map(LongWritable key, V1 value, Context context)
      throws IOException, InterruptedException {
    
	Context keyOnlyContext = new KeyOnlyContext(context);
	m_inputMapper.map(key, value, keyOnlyContext);
  }
}