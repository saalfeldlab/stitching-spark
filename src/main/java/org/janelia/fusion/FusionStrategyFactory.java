package org.janelia.fusion;

import net.imglib2.Interval;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;

public interface FusionStrategyFactory
{
	public static < T extends RealType< T > & NativeType< T > > FusionStrategy< T > createFusionStrategy(
			final FusionMode mode,
			final Interval targetInterval,
			final T type )
	{
		switch ( mode )
		{
		case MAX_MIN_DISTANCE:
			return new MaxMinDistanceFusionStrategy<>( targetInterval, type );
		case BLENDING:
			return new BlendingFusionStrategy<>( targetInterval, type );
		case MAX_INTENSITY:
			return new MaxIntensityFusionStrategy<>( targetInterval, type );
		default:
			throw new RuntimeException( "Unknown fusion mode" );
		}
	}
}
