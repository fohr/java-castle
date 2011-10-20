package com.acunu.castle;

import java.io.IOException;

/**
 * An exception from the Castle kernel module. The errno gives more information.
 * An enumeration of acceptable errnos is kept in the auto-generated class {@linkplain CastleError}
 */
@SuppressWarnings("serial")
public class CastleException extends IOException {
	static {
		System.load("/usr/lib64/java-castle/libCastleImpl.so");
		init_jni();
	}

	static native void init_jni();

	private final int errno;
	private final String msg;

	public CastleException(int errno, String msg) {
		this.errno = errno;
		this.msg = msg;
	}

	public CastleException(int errno, String msg, Throwable cause) {
		super(cause);
		this.errno = errno;
		this.msg = msg;
	}

	public CastleException(CastleError err, String msg, Throwable cause) {
		super(cause);
		this.errno = err.errno;
		this.msg = msg;
	}

	public CastleException(CastleError err, String msg) {
		this.errno = err.errno;
		this.msg = msg;
	}

	public int getErrno() {
		return errno;
	}

	@Override
	public String toString() {
		return String.format("CastleException(err=%d, msg=\"%s\")", errno, msg);
	}
}
