package kr.ac.mju.idpl.melon.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import kr.ac.mju.idpl.melon.MeLoN_ConfigurationKeys;

public class Utils {
	public static String parseMemoryString(String memory) {
		memory = memory.toLowerCase();
		int m = memory.indexOf('m');
		int g = memory.indexOf('g');
		if (-1 != m) {
			return memory.substring(0, m);
		}
		if (-1 != g) {
			return String.valueOf(Integer.parseInt(memory.substring(0, g)) * 1024);
		}
		return memory;
	}

	public static int getNumTotalTasks(Configuration conf) {
		return getAllJobTypes(conf).stream().mapToInt(type -> conf.getInt("melon." + type + ".instances", 0)).sum();
	}

	public static Set<String> getAllJobTypes(Configuration conf) {
		return conf.getValByRegex(MeLoN_ConfigurationKeys.INSTANCES_REGEX).keySet().stream().map(Utils::getTaskType)
				.collect(Collectors.toSet());
	}

	public static String getTaskType(String confKey) {
		Pattern instancePattern = Pattern.compile(MeLoN_ConfigurationKeys.INSTANCES_REGEX);
		Matcher instanceMatcher = instancePattern.matcher(confKey);
		if (instanceMatcher.matches()) {
			return instanceMatcher.group(1);
		} else {
			return null;
		}
	}

	public static Map<String, String> parseKeyValue(String[] keyValues) {
		Map<String, String> keyValue = new HashMap<>();
		if (keyValues == null) {
			return keyValue;
		}
		for (String kv : keyValues) {
			String trimmedKeyValue = kv.trim();
			int index = kv.indexOf('=');
			if (index == -1) {
				keyValue.put(trimmedKeyValue, "");
				continue;
			}
			String key = trimmedKeyValue.substring(0, index);
			String val = "";
			if (index < (trimmedKeyValue.length() - 1)) {
				val = trimmedKeyValue.substring(index + 1);
			}
			keyValue.put(key, val);
		}
		return keyValue;
	}

	public static boolean isArchive(String path) {
		File f = new File(path);
		int fileSignature = 0;
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(f, "r");
			fileSignature = raf.readInt();
		} catch (IOException e) {
			// handle if you like
		} finally {
			IOUtils.closeQuietly(raf);
		}
		return fileSignature == 0x504B0304 // zip
				|| fileSignature == 0x504B0506 // zip
				|| fileSignature == 0x504B0708 // zip
				|| fileSignature == 0x74657374 // tar
				|| fileSignature == 0x75737461 // tar
				|| (fileSignature & 0xFFFF0000) == 0x1F8B0000; // tar.gz
	}
}
