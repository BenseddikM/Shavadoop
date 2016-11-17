public class ProgressBar
{
	private static int lastPercent;

	public static void updatePercentageBar(float progress) {
		int percent = (int) Math.round(progress * 100);
		if (Math.abs(percent - lastPercent) >= 1) {
			StringBuilder template = new StringBuilder("\r[");
			for (int i = 0; i < 50; i++) {
				if (i < percent * .5) {
					template.append("=");
				} else if (i == percent * .5) {
					template.append(">");
				} else {
					template.append(" ");
				}
			}
			template.append("] %s   ");
			if (percent >= 100) {
				template.append("%n");
			}
			System.out.printf(template.toString(), percent + "%");
			lastPercent = percent;
		}
	}
}