package niagara.firehose;

import htmllayout.HtmlLayout;

import java.awt.Color;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Image;
import java.awt.Rectangle;
import java.awt.Toolkit;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.StringTokenizer;

import javax.swing.BorderFactory;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSlider;
import javax.swing.JTextArea;

class GameStatus {

	static String m_stFieldImageFile = "field.gif";
	static String m_stRunnerImageFile = "x.gif";
	int m_nPort = FirehoseConstants.LISTEN_PORT;
	String m_stHost;
	String m_stGameId = null;
	String m_stVisitors = new String("AWAY"), m_stHome = new String("HOME"),
			m_stScore;
	int m_nHomeScore = 0, m_nVisitorsScore = 0;
	int m_nHomeHit = 0, m_nVisitorsHit = 0;
	int m_nHomeError = 0, m_nVisitorsError = 0;
	int m_nInning = 1;
	FieldPanel m_fdp;
	JTextArea m_txaScore;
	JSlider m_sldSleep = new JSlider(100, 1000, 700);
	boolean m_fHomeUp = false;
	boolean m_fFrameGone = false;

	public GameStatus() {
		Image imgBase = Toolkit.getDefaultToolkit().createImage(
				GameStatus.m_stFieldImageFile);
		Image imgRunner = Toolkit.getDefaultToolkit().createImage(
				GameStatus.m_stRunnerImageFile);

		JFrame f = new JFrame("Game status");

		try {
			m_stHost = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException ex) {
			m_stHost = new String("chinook.cse.ogi.edu");
		}
		m_txaScore = new JTextArea(4, 22);
		m_txaScore.setForeground(Color.white);
		m_txaScore.setBackground(Color.black);
		m_txaScore.setEditable(false);

		m_fdp = new FieldPanel(imgBase, imgRunner);
		m_sldSleep.setOrientation(JSlider.HORIZONTAL);

		m_sldSleep.setMajorTickSpacing(300);
		m_sldSleep.setPaintTicks(true);
		m_sldSleep.setPaintLabels(true);
		m_sldSleep.setBorder(BorderFactory.createEmptyBorder(0, 0, 10, 0));

		Container c = f.getContentPane();
		c
				.setLayout(new HtmlLayout(
						"<table rows=2 cols=2><tr><td component=scoreboard><td horz=fit><table rows=2 cols=1><tr><td horz=center component=speedlabel><tr><td component=slider horz=center></table><tr><td component=m_fdp horz=max vert=max colspan=2>"));
		c.add(new JLabel("events / second"), "speedlabel");
		c.add(m_sldSleep, "slider");
		c.add(m_txaScore, "scoreboard");
		c.add(m_fdp, "m_fdp");

		f.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				m_fFrameGone = true;
			}
		});

		// f.setSize(new Dimension(620, 380));
		f.pack();
		f.setVisible(true);

		setScoreboard();
	}

	public void newDocument(String stDoc) {
		StringTokenizer sttDoc = new StringTokenizer(stDoc);
		String stToken, stThisGameId;

		if (m_fFrameGone == true)
			return;

		try {
			if (m_stGameId == null) {
				// Wait for a new game to come through, and use it
				stToken = sttDoc.nextToken();
				while (stToken.compareToIgnoreCase("<GAME_START>") != 0) {
					stToken = sttDoc.nextToken();
				}

				setNewGame(sttDoc);
			} else {
				// Look for documents with this gameid, then find the action
				// for it
				stToken = sttDoc.nextToken();
				while (stToken.compareToIgnoreCase("<GAME_END>") != 0
						&& stToken.compareToIgnoreCase("<GAMEID>") != 0
						&& stToken.compareToIgnoreCase("<NEW_INNING/>") != 0) {
					stToken = sttDoc.nextToken();
				}

				if (stToken.compareToIgnoreCase("<GAME_END>") == 0) {
					// Is it this gameid thats ending?
					while (stToken.compareToIgnoreCase("<GAMEID>") != 0) {
						stToken = sttDoc.nextToken();
					}

					stThisGameId = sttDoc.nextToken();
					if (m_stGameId.startsWith(stThisGameId.substring(0, 3))) {
						// Yup. kill this gameid so we can find another
						System.out.println("\tOur game ended.");
						m_stGameId = null;
					}
				} else if (stToken.compareToIgnoreCase("<NEW_INNING/>") == 0) {
					// Is it this gameid thats ending?
					while (stToken.compareToIgnoreCase("<GAMEID>") != 0) {
						stToken = sttDoc.nextToken();
					}

					stThisGameId = sttDoc.nextToken();
					if (m_stGameId.startsWith(stThisGameId.substring(0, 3))) {
						// Yup. Next team is up
						m_fHomeUp = !m_fHomeUp;
						if (m_fHomeUp == false)
							m_nInning++;

						handlePlay(sttDoc);
					}
				} else {
					stThisGameId = sttDoc.nextToken();
					if (m_stGameId.compareToIgnoreCase(stThisGameId) == 0) {
						handlePlay(sttDoc);
					}
				}
			}
		} catch (Exception e) {
			;
		}
	}

	private void handlePlay(StringTokenizer sttDoc) {
		String stToken;
		boolean fReadPlay = true;
		boolean fRunner, fRun, fHit, fError;

		try {
			while (fReadPlay) {
				stToken = sttDoc.nextToken();
				fRunner = (stToken.compareToIgnoreCase("<BASE_RUNNERS>") == 0);
				fRun = (stToken.compareToIgnoreCase("<RUNSCORED>") == 0);
				fHit = (stToken.compareToIgnoreCase("<HIT>") == 0);
				fError = (stToken.compareToIgnoreCase("<ERROR>") == 0);
				while ((fRunner | fRun | fHit | fError) == false) {
					stToken = sttDoc.nextToken();

					fRunner = (stToken.compareToIgnoreCase("<BASE_RUNNERS>") == 0);
					fRun = (stToken.compareToIgnoreCase("<RUNSCORED>") == 0);
					fHit = (stToken.compareToIgnoreCase("<HIT>") == 0);
					fError = (stToken.compareToIgnoreCase("<ERROR>") == 0);
				}

				if (fRun) {
					addRun();
				} else if (fHit) {
					addHit();
				} else if (fError) {
					addError();
				} else {
					fReadPlay = false;
				}
			}

			setRunners(sttDoc);
		} catch (Exception e) {
			;
		}
	}

	private void setRunners(StringTokenizer sttDoc) {
		String stToken = new String("");
		boolean f1 = false, f2 = false, f3 = false;
		Rectangle rct1 = new Rectangle(410, 161, 30, 30);
		Rectangle rct2 = new Rectangle(292, 61, 30, 30);
		Rectangle rct3 = new Rectangle(174, 161, 30, 30);

		try {
			// Found the BaseRunners element. See what runners there are
			while (stToken.compareToIgnoreCase("</BASE_RUNNERS>") != 0) {
				stToken = sttDoc.nextToken();
				f1 |= (stToken.compareToIgnoreCase("<RUNNER_FIRSTBASE>") == 0);
				f2 |= (stToken.compareToIgnoreCase("<RUNNER_SECONDBASE>") == 0);
				f3 |= (stToken.compareToIgnoreCase("<RUNNER_THIRDBASE>") == 0);
			}

			m_fdp.setState(f1, f2, f3);
			m_fdp.repaint(rct1);
			m_fdp.repaint(rct2);
			m_fdp.repaint(rct3);

			// Give the UI some time to catch up
			int iEventsPerSec = m_sldSleep.getValue();
			Thread.sleep(10000 / iEventsPerSec);
		} catch (Exception e) {
			System.out.println("\tException - " + e.toString());
			;
		}
	}

	private void setScoreboard() {
		String stVisitors, stHome;

		if (m_fHomeUp) {
			stHome = new String("*" + m_stHome);
			stVisitors = m_stVisitors;
		} else {
			stHome = m_stHome;
			stVisitors = new String("*" + m_stVisitors);
		}

		m_stScore = new String("\tR\tH\tE\n" + stVisitors + "\t"
				+ m_nVisitorsScore + "\t" + m_nVisitorsHit + "\t"
				+ m_nVisitorsError + "\n" + stHome + "\t" + m_nHomeScore + "\t"
				+ m_nHomeHit + "\t" + m_nHomeError + "\n" + "\tInning "
				+ m_nInning);

		m_txaScore.setText(m_stScore);
	}

	private void addRun() {
		if (m_fHomeUp == true) {
			m_nHomeScore++;
		} else {
			m_nVisitorsScore++;
		}

		setScoreboard();
	}

	private void addHit() {
		if (m_fHomeUp == true) {
			m_nHomeHit++;
		} else {
			m_nVisitorsHit++;
		}

		setScoreboard();
	}

	private void addError() {
		if (m_fHomeUp == true) {
			m_nVisitorsError++;
		} else {
			m_nHomeError++;
		}

		setScoreboard();
	}

	private void setNewGame(StringTokenizer sttDoc) {
		String stToken;

		m_nVisitorsScore = 0;
		m_nVisitorsHit = 0;
		m_nVisitorsError = 0;
		m_nHomeScore = 0;
		m_nHomeHit = 0;
		m_nHomeError = 0;
		m_nInning = 1;

		try {
			// We have a game start. Get the game id and hold onto it
			stToken = sttDoc.nextToken();
			while (stToken.compareToIgnoreCase("<GAMEID>") != 0) {
				stToken = sttDoc.nextToken();
			}
			m_stGameId = sttDoc.nextToken();
			System.out.println("GameId - '" + m_stGameId + "'");

			// Get visiting team
			while (stToken.compareToIgnoreCase("<VISITING_TEAM>") != 0) {
				stToken = sttDoc.nextToken();
			}
			m_stVisitors = new String(sttDoc.nextToken());

			// Get home team
			while (stToken.compareToIgnoreCase("<HOME_TEAM>") != 0) {
				stToken = sttDoc.nextToken();
			}
			m_stHome = new String(sttDoc.nextToken());

			setScoreboard();
		} catch (Exception e) {
			;
		}
	}
}

@SuppressWarnings("serial")
class FieldPanel extends JPanel {
	Image m_imgField, m_imgRunner;
	boolean m_fFirst = false, m_fSecond = false, m_fThird = false;

	public FieldPanel(Image imgField, Image imgRunner) {
		setPreferredSize(new Dimension(600, 300));
		m_imgField = imgField;
		m_imgRunner = imgRunner;
	}

	public void setState(boolean fFirst, boolean fSecond, boolean fThird) {
		m_fFirst = fFirst;
		m_fSecond = fSecond;
		m_fThird = fThird;
	}

	public void paintComponent(Graphics g) {
		super.paintComponent(g); // paint background

		// Draw field.
		g.drawImage(m_imgField, 0, 0, this);

		// Draw Batter
		g.drawImage(m_imgRunner, 270, 276, this);

		if (m_fFirst) {
			// Draw Runner on FirstBase
			g.drawImage(m_imgRunner, 410, 161, this);
		}
		if (m_fSecond) {
			// Draw Runner on SecondBase
			// System.out.println("Second base XXX");
			g.drawImage(m_imgRunner, 292, 61, this);
		}
		if (m_fThird) {
			// Draw Runner on ThirdBase
			g.drawImage(m_imgRunner, 174, 161, this);
		}
	}
}
