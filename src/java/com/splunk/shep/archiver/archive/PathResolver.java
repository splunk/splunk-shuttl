package com.splunk.shep.archiver.archive;

import java.net.URI;
import java.net.URISyntaxException;

import com.splunk.shep.archiver.model.Bucket;

public class PathResolver {

    private final ArchiveConfiguration configuration;

    public PathResolver(ArchiveConfiguration configuration) {
	this.configuration = configuration;
    }

    public URI resolveArchivePath(Bucket bucket) {
	String uri = "file:/" + configuration.getArchivingRoot() + "/"
		+ configuration.getClusterName() + "/"
		+ configuration.getServerName() + "/" + bucket.getIndex() + "/"
		+ bucket.getFormat() + "/" + bucket.getName();
	return getUriSafe(uri);
    }

    private URI getUriSafe(String uri) {
	try {
	    return new URI(uri);
	} catch (URISyntaxException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }
}
