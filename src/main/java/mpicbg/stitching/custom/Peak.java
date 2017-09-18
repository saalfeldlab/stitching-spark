package mpicbg.stitching.custom;

import mpicbg.imglib.algorithm.fft.custom.PhaseCorrelationPeak;
import mpicbg.imglib.algorithm.scalespace.DifferenceOfGaussian.SpecialPoint;
import mpicbg.imglib.algorithm.scalespace.DifferenceOfGaussianPeak;
import mpicbg.imglib.type.numeric.real.FloatType;

public class Peak extends DifferenceOfGaussianPeak<FloatType>
{
	final PhaseCorrelationPeak peak;

	public Peak( final PhaseCorrelationPeak peak )
	{
		super( peak.getOriginalInvPCMPosition(), new FloatType( peak.getPhaseCorrelationPeak() ), SpecialPoint.MAX );

		this.peak = peak;
	}

	public PhaseCorrelationPeak getPCPeak() { return peak; }

}
