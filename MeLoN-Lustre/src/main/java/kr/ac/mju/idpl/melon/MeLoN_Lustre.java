package kr.ac.mju.idpl.melon;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kr.ac.mju.idpl.melon.util.Utils;

public class MeLoN_Lustre {
	private static final Logger LOG = LoggerFactory.getLogger(MeLoN_Lustre.class);

	public static void mkdironLustre(String appId) {
		String appIdDir = MeLoN_Constants.LUSTRE_FILESYSTEM_URI + File.separator + appId;
		String[] mkdirAppId = { "mkdir", appIdDir };
		LOG.info("Make Directory in Lustre");
		lustreCommand(mkdirAppId);
	}

	public static void lustreCommand(String[] cmd) {
		try {
			Process p = Runtime.getRuntime().exec(cmd);
			p.getErrorStream().close();
			p.getInputStream().close();
			p.getOutputStream().close();
			p.waitFor();
		} catch (Exception e) {
			LOG.info("Failed to execute command");
		}
	}
}
