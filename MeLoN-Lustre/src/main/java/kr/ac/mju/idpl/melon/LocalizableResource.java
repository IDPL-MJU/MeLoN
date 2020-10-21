package kr.ac.mju.idpl.melon;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * Parse container resources of format:
 * SOURCE_FILE_PATH::PATH_IN_CONTAINER#archive only SOURCE_FILE_PATH is
 * required.
 *
 * SOURCE_FILE_PATH: location of the file to be localized to containers. This
 * could be either local resources or remote resources. PATH_IN_CONTAINER:
 * optional, default to source file name. If specified, source_file_path will be
 * localized as name file_in_container in container. ARCHIVE: if #archive is put
 * at the end, the file will be uploaded as ARCHIVE type and unzipped upon
 * localization.
 */
public class LocalizableResource {
	private String rsrcFormedString;
	private Path rsrcPath;
	private FileStatus rsrcFileStatus;
	private LocalResourceType rsrcType;
	private boolean isDirectory;
	private String localizedFileName;

	public LocalizableResource(String rsrcFormedString, FileSystem fs) throws IOException, ParseException {
		this.rsrcFormedString = rsrcFormedString;
		this.parse(fs);
	}

	public Path getRsrcPath() {
		return rsrcPath;
	}

	public String getLocalizedFileName() {
		return localizedFileName;
	}

	public boolean isDirectory() {
		return isDirectory;
	}
	
	public boolean isArchive() {
		return rsrcType == LocalResourceType.ARCHIVE;
	}
	
	public boolean isLocalFile() {
		return new Path(rsrcFormedString).toUri().getScheme() == null;
	}

	private void parse(FileSystem fs) throws IOException, ParseException {
		String filePath = rsrcFormedString;
		if (rsrcFormedString.toLowerCase().endsWith(MeLoN_Constants.ARCHIVE_SUFFIX)) {
			rsrcType = LocalResourceType.ARCHIVE;
			filePath = rsrcFormedString.substring(0,
					rsrcFormedString.length() - MeLoN_Constants.ARCHIVE_SUFFIX.length());
		} else {
			rsrcType = LocalResourceType.FILE;
		}
		String[] tuple = filePath.split(MeLoN_Constants.RESOURCE_DIVIDER);

		if (tuple.length <= 2) {
			rsrcPath = new Path(tuple[0]);
			if (isLocalFile()) {
				FileSystem localFs = FileSystem.getLocal(fs.getConf());
				rsrcFileStatus = localFs.getFileStatus(rsrcPath);
			} else {
				rsrcFileStatus = fs.getFileStatus(rsrcPath);
			}
			localizedFileName = rsrcPath.getName();
			if(tuple.length == 2){
				localizedFileName = tuple[1];
			}
			if(rsrcFileStatus.isDirectory()) {
				isDirectory = true;
			}
		}else {
			throw new ParseException("Failed to parse file: " + rsrcFormedString);
		}
	}

	public LocalResource toLocalResource() {
		if (isDirectory) {	
			throw new RuntimeException("Resource is a directory and cannot be converted to LocalResource.");
		}
		return LocalResource.newInstance(
				ConverterUtils.getYarnUrlFromURI(URI.create(rsrcFileStatus.getPath().toString())), 
				rsrcType,
				LocalResourceVisibility.PRIVATE, 
				rsrcFileStatus.getLen(), 
				rsrcFileStatus.getModificationTime()
				);
	}
}
