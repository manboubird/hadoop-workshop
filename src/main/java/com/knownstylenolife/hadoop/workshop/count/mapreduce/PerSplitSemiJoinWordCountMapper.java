package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.writable.PerSplitSemiJoinMapOutputKeyWritable;
import com.rapleaf.lightweight_trie.ImmutableStringRadixTreeMap;
import com.rapleaf.lightweight_trie.StringRadixTreeMap;

public class PerSplitSemiJoinWordCountMapper extends Mapper<LongWritable, Text, PerSplitSemiJoinMapOutputKeyWritable, LongWritable> {
	
	private Log LOG = LogFactory.getLog(PerSplitSemiJoinWordCountMapper.class);

	public static final String WORDS_REGEX = "([\\w-]+)([^\\w-]|$)";

	private PerSplitSemiJoinMapOutputKeyWritable outputKey;
	private LongWritable outputValue;
	private StringRadixTreeMap<Long> wordCountMap;
		
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LogUtil.setLogLevel(LOG, context.getConfiguration());
		outputKey = new PerSplitSemiJoinMapOutputKeyWritable();
		outputValue = new LongWritable(0L);
		wordCountMap = new StringRadixTreeMap<Long>();
	}
	
	/**
	 *  Three memory efficiency improvement
	 *  <ol>
	 *    <li> Use Trie-tree data structure to keep join key and link instead of HashMap.
	 *		(Ref. <a href="http://blog.rapleaf.com/dev/2011/04/12/lightweight-trie/">Lightweight Trie</a>)
	 *    </li>
	 *    <li>
	 *     filter un-used key in master data.
	 *    </li>
	 *    <li>
	 *     per split master data.
	 *    </li>
	 *  </ol>
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		if(LOG.isDebugEnabled()) { LOG.debug("[ key = \"" + key.toString() + "\" ]" + "[ value = \"" + value.toString() + "\"]"); }
		Matcher matcher = Pattern.compile(WORDS_REGEX).matcher(value.toString());
		while(matcher.find()) {
			String word = matcher.group(1);
			Long count = wordCountMap.get(word);
		    wordCountMap.put(word, count == null ? 1 : ++count);
			if(LOG.isDebugEnabled()) { 
				LOG.debug(" put word[ word = \"" + word + "\" ][ count = " + count + "]"); 
			}

		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for(File joinFile : getSplitJoinFiles(context.getConfiguration())) {
			for(String joinKey : createJoinKeySet(joinFile)){
				Long count = wordCountMap.get(joinKey);
				if(count != null) {
					outputKey.word = joinKey;
					outputKey.link = null;
					outputValue.set(count.longValue());
		        	if(LOG.isDebugEnabled()) { LOG.debug("  [ outputKey = [ word=" + outputKey.word + " ][ link=" + outputKey.link + " ]" + "[ outputValue = \"" + outputValue.get() + "\" ]"); }
					context.write(outputKey, outputValue);
				}
			}	
		}
	}
	
	private File[] getSplitJoinFiles(Configuration conf) {
        String joinFiles = conf.get(PerSplitSemiJoinWordCountMapper.class.getName() + ".joinFiles", "");
        Preconditions.checkState(!"".equals(joinFiles));
        File joinFileDir = new File(joinFiles);
        Preconditions.checkState(joinFileDir.exists() && joinFileDir.isDirectory());
		if(LOG.isDebugEnabled()) {
			LOG.debug("joinFiles Path = " + joinFileDir.getAbsolutePath());
			LOG.debug("joinFiles list : " + Joiner.on("\n").join(joinFileDir.list()));
		}
		return joinFileDir.listFiles(new FileFilter() {
			public boolean accept(File pathname) {
				return pathname.getName().startsWith("links.txt.");
			}
		});
	}

	private Set<String> createJoinKeySet(File file) throws IOException {
	    StringRadixTreeMap<String> joinMap = new StringRadixTreeMap<String>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "utf-8"));
            String line;
            while((line = reader.readLine()) != null) {
                String[] splitted = line.split("\t");
				Preconditions.checkState(splitted.length == 2, "Illegal line format. \"" + line + "\"");
				joinMap.put(splitted[0], splitted[1]);
            }
        } finally {
            if(reader != null) { reader.close(); }
        }
		if(LOG.isDebugEnabled()) {
			LOG.debug("join filename = " + file.getName());
			LOG.debug("join keyset : \n" + Joiner.on("\n").join(joinMap.keySet()));
		}
        return new ImmutableStringRadixTreeMap<String>(joinMap).keySet();
	}
}