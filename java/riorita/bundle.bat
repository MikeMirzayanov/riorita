call mvn.cmd validate --batch-mode
call mvn.cmd -Dfile.encoding=UTF-8 -DcreateChecksum=true clean source:jar javadoc:jar repository:bundle-create install --batch-mode %*
