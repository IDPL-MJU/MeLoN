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

	public static void extractVenvandSrc(String dst) {
		File venvZip = new File(dst + File.separator + MeLoN_Constants.PYTHON_VENV_ZIP);
		File srcZip = new File(dst + File.separator + MeLoN_Constants.MELON_SRC_ZIP_NAME);
		if (venvZip.exists()) {
			LOG.info("Unpacking Python virtual environment..");
			Utils.unzipArchive(dst + File.separator + MeLoN_Constants.PYTHON_VENV_ZIP, dst + File.separator);
		} else {
			LOG.info("No virtual environment uploaded.");
		}
		if (srcZip.exists()) {
			LOG.info("Unpacking src Folder..");
			Utils.unzipArchive(dst + File.separator + MeLoN_Constants.MELON_SRC_ZIP_NAME,
					dst + File.separator + MeLoN_Constants.SRC_DIR + File.separator);
		} else {
			LOG.info("No src folder uploaded.");
		}
	}
}
