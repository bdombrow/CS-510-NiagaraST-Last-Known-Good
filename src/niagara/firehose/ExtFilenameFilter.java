package niagara.firehose;

//Filter out files which don't have the correct extension. This
// is used by File.list, which lists files in a directory
class ExtFilenameFilter implements java.io.FilenameFilter {
  private String m_stExt;
  private int m_cchExt;

  public ExtFilenameFilter(String st) {
    m_stExt = new String(st);
    m_cchExt = m_stExt.length();
  }

  public boolean accept(java.io.File dir, java.lang.String name) {
      return name.regionMatches(true,
				name.length() - m_cchExt,
				m_stExt,
				0,
				m_cchExt);
  }
}

