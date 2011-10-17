package com.acunu.castle;

import java.io.IOException;

/**
 * An exception from the Castle kernel module. The errno gives more information.
 * To diagnose meaning, try googling errno.h and reading dmesg.
 */
public class CastleException extends IOException {
	static {
		System.load("/usr/lib64/java-castle/libCastleImpl.so");
		init_jni();
	}

	static native void init_jni();

	public enum Error {
		ABSENT(129, "Object does not exist"),
		MERGE_ORDER(130, "Array inputs to merge not sequential");

		private final int code;
		private final String description;

		private Error(int code, String description) {
			this.code = code;
			this.description = description;
		}

		public String getDescription() {
			return description;
		}

		public int getCode() {
			return code;
		}

		@Override
		public String toString() {
			return code + ": " + description;
		}
	}

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
	
	public CastleException(Error err, String msg, Throwable cause) {
		super(cause);
		this.errno = err.getCode();
		this.msg = msg;
	}

	public CastleException(Error err, String msg) {
		this.errno = err.getCode();
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
