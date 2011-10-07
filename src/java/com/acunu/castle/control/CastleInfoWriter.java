package com.acunu.castle.control;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.logging.Logger;

import com.acunu.castle.control.CastleInfoWriter.DAInfoWriter;
import com.acunu.util.Function;
import com.acunu.util.Utils;

/**
 * Provide a comprehensible ASCII report on a castle.
 * 
 * @author andrewbyde
 */
public class CastleInfoWriter extends AbstractCastleListener<DAInfoWriter> {
	protected static Logger log = Logger.getLogger(CastleInfoWriter.class
			.getName());

	protected final Function<Integer, PrintWriter> writerMaker;
	protected final int delay;

	protected int pageWidth = 89;

	/**
	 * Run an info writer. TODO -- set destination based on args.
	 */
	public static void main(String[] args) {

		try {
			CastleControlServer cv = new CastleControlServerImpl();

			// write to a file particular to the da in question.
			Function<Integer, PrintWriter> writerMaker = new Function<Integer, PrintWriter>() {
				public PrintWriter evaluate(Integer daId) {
					if (daId == null)
						return null;
					String filename = "/var/log/castle/da_" + hex(daId);
					log.info("Open log file for da '" + hex(daId) + "' at " + filename);
					try {
						FileOutputStream fs = new FileOutputStream(filename);
						PrintWriter p = new PrintWriter(fs) {
							public void println(String s) {
								super.println(s);
								System.out.println(s);
							}
						};
						return p;
					} catch (IOException e) {
						log.severe(e.getMessage());
						return null;
					}
				}
			};
			CastleListener cl = new CastleInfoWriter(cv, writerMaker, 500);
			cv.setController(cl);

			// do nothing -- wait for threads to terminate.
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Constructor in which we set up a function to create print writers
	 * whenever a refresh is in order, and a maximum delay between calls to
	 * refresh.
	 * 
	 * @param writerMaker
	 *            a function taking the DA id and creating a print writer for
	 *            it. For example, might open a file with the id appended to the
	 *            filename.
	 * @param delay
	 *            minimum delay in milliseconds between calls to refresh. If
	 *            there has been no refresh-worthy event then the writers will
	 *            not be called, but on the other hand if there are many calls,
	 *            then we refresh at most once per 'delay' milliseconds. Relies
	 *            on listening to a castle server to hear when interesting
	 *            things are happening.
	 */
	public CastleInfoWriter(CastleView view,
			Function<Integer, PrintWriter> writerMaker, int delay) {
		this.writerMaker = writerMaker;
		this.delay = delay;
		setServer(view);
	}


	@Override
	protected DAInfoWriter makeFacet(int daId) {
		return new DAInfoWriter(daId);
	}

	/**
	 * A class to periodically write out a text representation of a DA.
	 * Refreshes on demand and at most every 'delta' milliseconds, where delta
	 * is a constructor parameter.
	 */
	class DAInfoWriter implements DAListener {
		private final int daId;
		private Boolean refreshNeeded = true;

		DAInfoWriter(final int daId) {
			this.daId = daId;
			log.info("New writer for " + daId);
			
			// create a thread to limit the number of refreshes that are done.
			Thread t = new Thread() {
				public void run() {
					while (true) {
						Utils.waitABit(delay);
						boolean r = false;
						synchronized (refreshNeeded) {
							r = refreshNeeded;
							if (r)
								refreshNeeded = false;
						}
						if (r) {
							try {
								PrintWriter writer = writerMaker.evaluate(daId);
								log.fine("refresh " + daId);
								String s = note();
								writer.println(s);
								writer.flush();
								writer.close();
							} catch (Exception e) {
								log.severe(e.getMessage());
							}
						}
					}
				}
			};
			t.setName("writer_" + hex(daId));
			t.start();
		}

		public void refresh() {
			synchronized (refreshNeeded) {
				refreshNeeded = true;
			}
		}

		/**
		 * A page describing the state of the DA
		 */
		public String note() {
			if (server == null)
				return "Waiting for server";

			StringBuilder sb = new StringBuilder();
			synchronized (CastleControlServerImpl.syncLock) {

				DAView daView = server.projectView(daId);
				if (daView == null)
					return "No such da '" + daId + "'";

				DAInfo daInfo = daView.getDAInfo();
				if (daInfo == null)
					return "Waiting for data";

				// arrays
				StringBuilder sba = new StringBuilder();
				int i = 0;
				int lines = 0;
				int levelLines = 0;
				for (Integer id : daInfo.arrayIds) {
					ArrayInfo info = daView.getArrayInfo(id);
					if (info != null) {

						// id
						sba.append(Utils.pad(5, "" + DAObject.hex(info.id)));

						// 5 chars...

						// dynamics
						if (info.mergeState == ArrayInfo.MergeState.INPUT)
							sba.append(" *   ");
						else if (info.mergeState == ArrayInfo.MergeState.OUTPUT)
							sba.append(" `-> ");
						else
							sba.append("     ");

						// 10 chars...

						// size
						sba.append(" ").append(
								Utils.pad(9,
										Utils.toStringSize(info.maxInBytes())));

						// 20 chars...

						// progress
						if (info.mergeState == ArrayInfo.MergeState.INPUT)
							sba.append("|")
									.append(Utils.pad(10,
											progress(info.progress())))
									.append("|");
						else
							sba.append("|")
									.append(Utils.pad(
											progress(info.progress()), 10))
									.append("|");

						// 31 chars

						// ve list
						Collection<Integer> veIds = info.valueExIds;
						StringBuilder sbb = new StringBuilder();
						for (Integer vId : veIds) {
							ValueExInfo vInfo = daView.getValueExInfo(vId);
							sbb.append(" ").append(vInfo.note());
						}
						String s = sbb.toString();
						sba.append(Utils.pad(pageWidth - 31, s));

						sba.append("\n");
						lines++;
						levelLines++;
					}
					i++;
				}

				while (lines++ < 20)
					sb.append("\n");
				sb.append(sba);
				sb.append("\n");

				// VE list
				int colWidth = 15;
				int nCols = pageWidth / colWidth;
				if (daInfo.valueExIds != null) {
					sb.append("Value Extents:\n");
					int colHeight = Math.max(10, daInfo.valueExIds.size()
							/ nCols);
					String[] vLines = new String[colHeight];
					for (int j = 0; j < colHeight; j++) {
						vLines[j] = "";
					}
					int vI = 0;
					for (Integer vid : daInfo.valueExIds) {
						int col = vI / colHeight;
						int row = vI - colHeight * col;
						ValueExInfo vInfo = daView.getValueExInfo(vid);
						if (vInfo != null) {
							vLines[row] += Utils.pad(vInfo.note(), colWidth);
							vI++;
						}
					}
					for (int j = 0; j < colHeight; j++) {
						sb.append(vLines[j]).append("\n");
					}
				}

				lines = 0;
				// merge index
				sb.append("\nMerges:\n  id   work   done arrays\n");
				for (Integer id : daInfo.mergeIds) {
					MergeInfo m = daView.getMergeInfo(id);
					if (m == null)
						continue;
					sb.append(Utils.pad(4, DAObject.hex(id)));
					sb.append(" " + Utils.toStringSize(m.workTotal));
					sb.append(" " + Utils.toStringSize(m.workDone));
					sb.append(" " + DAObject.hex(m.inputArrayIds) + " -> "
							+ DAObject.hex(m.outputArrayIds) + "\n");
					lines++;
				}
			}

			return sb.toString();
		}

		@Override
		public void newArray(ArrayInfo arg0) {
			refresh();
		}

		@Override
		public void workDone(int arg0, int arg1, long arg2, boolean arg3) {
			refresh();
		}
	}

	private static String[] progress = new String[] { "", "#", "##", "###",
			"####", "#####", "######", "#######", "########", "#########",
			"##########" };

	/**
	 * ASCII representation of 'progress' measured between zero and one. Equates
	 * to a string of n hash characters, n \in [0, 10]
	 */
	public static String progress(double x) {
		if (x < 0)
			x = 0;
		else if (x > 1)
			x = 1;
		int n = (int) Math.round((progress.length - 1) * x);
		return progress[n];
	}

}
