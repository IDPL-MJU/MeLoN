package kr.ac.mju.idpl.melon;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kr.ac.mju.idpl.melon.util.Utils;

public class MeLoN_Lustre {
	private static final Logger LOG = LoggerFactory.getLogger(MeLoN_Lustre.class);
	
	public static void copyToLustre(String appId) {
		String appIdDir = MeLoN_Constants.LUSTRE_FILESYSTEM_URI + File.separator + appId;
		String[] mkdirAppId = { "mkdir", appIdDir};
		String[] copyVenv = {
				"cp",
				MeLoN_Constants.PYTHON_VENV_ZIP,
				appIdDir
		};
		String[] copySrc = {
				"cp",
				"-r",
				MeLoN_Constants.MELON_SRC_DIR,
				appIdDir
		};
		LOG.info("Make Directory in Lustre");
		lustreCommand(mkdirAppId);
		LOG.info("Copy Venv.zip to Lustre");
		lustreCommand(copyVenv);
		LOG.info("Copy src folder to Lustre");
		lustreCommand(copySrc);
		LOG.info("Unzip venv.zip");
		//extractVenv(appIdDir);
	}
	
	public static void lustreCommand(String[] cmd) {
		try {
			Process p = Runtime.getRuntime().exec(cmd);
			p.getErrorStream().close();
			p.getInputStream().close();
			p.getOutputStream().close();
			p.waitFor();
		}catch(Exception e) {
			LOG.info("Failed to execute command");
		}
	}
	
	public static void extractVenv(String dst) {
		File venvZip = new File(dst + File.separator + MeLoN_Constants.PYTHON_VENV_ZIP);
		if (venvZip.exists() && venvZip.isFile()) {
			LOG.info("Unpacking Python virtual environment..");
			Utils.unzipArchive(dst + File.separator + MeLoN_Constants.PYTHON_VENV_ZIP, dst + File.separator);
		} else {
			LOG.info("No virtual environment uploaded.");
		}
	}
}
