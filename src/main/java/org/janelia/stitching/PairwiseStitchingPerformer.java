package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import fiji.stacks.Hyperstack_rearranger;
import ij.ImagePlus;
import ij.gui.Roi;
import mpicbg.imglib.algorithm.fft.PhaseCorrelation;
import mpicbg.imglib.algorithm.fft.PhaseCorrelationPeak;
import mpicbg.imglib.algorithm.scalespace.DifferenceOfGaussianPeak;
import mpicbg.imglib.algorithm.scalespace.SubpixelLocalization;
import mpicbg.imglib.cursor.LocalizableByDimCursor;
import mpicbg.imglib.cursor.LocalizableCursor;
import mpicbg.imglib.custom.PointValidator;
import mpicbg.imglib.image.Image;
import mpicbg.imglib.image.ImageFactory;
import mpicbg.imglib.image.display.imagej.ImageJFunctions;
import mpicbg.imglib.multithreading.Chunk;
import mpicbg.imglib.multithreading.SimpleMultiThreading;
import mpicbg.imglib.type.numeric.RealType;
import mpicbg.imglib.type.numeric.integer.UnsignedByteType;
import mpicbg.imglib.type.numeric.integer.UnsignedShortType;
import mpicbg.imglib.type.numeric.real.FloatType;
import mpicbg.stitching.Peak;
import mpicbg.stitching.StitchingParameters;
import stitching.utils.Log;

/**
 * Pairwise Stitching of two ImagePlus using ImgLib1 and PhaseCorrelation.
 * It deals with aligning two slices (2d) or stacks (3d) having an arbitrary
 * amount of channels. If the ImagePlus contains several time-points it will
 * only consider the first time-point as this requires global optimization of
 * many independent 2d/3d <-> 2d/3d alignments.
 *
 * @author Stephan Preibisch (stephan.preibisch@gmx.de)
 *
 */

// based on PairWiseStitchingImgLib from Fiji's Stitching plugin repository

public class PairwiseStitchingPerformer
{
	static int numThreads;
	public static void setThreads( final int n )
	{
		numThreads = n;
	}

	public static SerializablePairWiseStitchingResult[] stitchPairwise( final ImagePlus imp1, final ImagePlus imp2,
			Roi roi1, Roi roi2, final int[] roiZ1, final int[] roiZ2,
			final int timepoint1, final int timepoint2, final StitchingParameters params, final int numHighestPeaks,
			final PointValidator searchRadiusPointValidator, final long[][] roiToTileOffset, final double[] globalOffset,
			final double[] stageOffset, final long[] confidenceIntervalMin, final long[] confidenceIntervalMax )
	{
		SerializablePairWiseStitchingResult[] result = null;
		roi1 = getOnlyRectangularRoi( roi1 );
		roi2 = getOnlyRectangularRoi( roi2 );

		// can both images be wrapped into imglib without copying
		final boolean canWrap = !StitchingParameters.alwaysCopy && canWrapIntoImgLib( imp1, roi1, params.channel1 ) && canWrapIntoImgLib( imp2, roi2, params.channel2 )
				&& roiZ1 == null && roiZ2 == null;


		//
		// the ugly but correct way into generic programming...
		//
		if ( canWrap )
		{
			if ( imp1.getType() == ImagePlus.GRAY32 )
			{
				final Image<FloatType> image1 = getWrappedImageFloat( imp1, params.channel1, timepoint1 );

				if ( imp2.getType() == ImagePlus.GRAY32 )
					result = performStitching( image1, getWrappedImageFloat( imp2, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else if ( imp2.getType() == ImagePlus.GRAY16 )
					result = performStitching( image1, getWrappedImageUnsignedShort( imp2, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else if ( imp2.getType() == ImagePlus.GRAY8 )
					result = performStitching( image1, getWrappedImageUnsignedByte( imp2, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else
					Log.error( "Unknown image type: " + imp2.getType() );

				image1.close();
			}
			else if ( imp1.getType() == ImagePlus.GRAY16 )
			{
				final Image<UnsignedShortType> image1 = getWrappedImageUnsignedShort( imp1, params.channel1, timepoint1 );

				if ( imp2.getType() == ImagePlus.GRAY32 )
					result = performStitching( image1, getWrappedImageFloat( imp2, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else if ( imp2.getType() == ImagePlus.GRAY16 )
					result = performStitching( image1, getWrappedImageUnsignedShort( imp2, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else if ( imp2.getType() == ImagePlus.GRAY8 )
					result = performStitching( image1, getWrappedImageUnsignedByte( imp2, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else
					Log.error( "Unknown image type: " + imp2.getType() );

				image1.close();
			}
			else if ( imp1.getType() == ImagePlus.GRAY8 )
			{
				final Image<UnsignedByteType> image1 = getWrappedImageUnsignedByte( imp1, params.channel1, timepoint1 );

				if ( imp2.getType() == ImagePlus.GRAY32 )
					result = performStitching( image1, getWrappedImageFloat( imp2, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else if ( imp2.getType() == ImagePlus.GRAY16 )
					result = performStitching( image1, getWrappedImageUnsignedShort( imp2, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else if ( imp2.getType() == ImagePlus.GRAY8 )
					result = performStitching( image1, getWrappedImageUnsignedByte( imp2, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else
					Log.error( "Unknown image type: " + imp2.getType() );

				image1.close();
			}
			else
			{
				Log.error( "Unknown image type: " + imp1.getType() );
			}
		}
		else
		{
			final ImageFactory<UnsignedByteType> imgFactoryByte = new ImageFactory<>( new UnsignedByteType(), StitchingParameters.phaseCorrelationFactory );
			final ImageFactory<UnsignedShortType> imgFactoryShort = new ImageFactory<>( new UnsignedShortType(), StitchingParameters.phaseCorrelationFactory );
			final ImageFactory<FloatType> imgFactoryFloat = new ImageFactory<>( new FloatType(), StitchingParameters.phaseCorrelationFactory );

			if ( imp1.getType() == ImagePlus.GRAY32 )
			{
				final Image< FloatType > image1 = getImage( imp1, roi1, roiZ1, imgFactoryFloat, params.channel1, timepoint1 );

				if ( imp2.getType() == ImagePlus.GRAY32 )
					result = performStitching( image1, getImage( imp2, roi2, roiZ2, imgFactoryFloat, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else if ( imp2.getType() == ImagePlus.GRAY16 )
					result = performStitching( image1, getImage( imp2, roi2, roiZ2, imgFactoryShort, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else if ( imp2.getType() == ImagePlus.GRAY8 )
					result = performStitching( image1, getImage( imp2, roi2, roiZ2, imgFactoryByte, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else
					Log.error( "Unknown image type: " + imp2.getType() );
			}
			else if ( imp1.getType() == ImagePlus.GRAY16 )
			{
				final Image< UnsignedShortType > image1 = getImage( imp1, roi1, roiZ1, imgFactoryShort, params.channel1, timepoint1 );

				if ( imp2.getType() == ImagePlus.GRAY32 )
					result = performStitching( image1, getImage( imp2, roi2, roiZ2, imgFactoryFloat, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else if ( imp2.getType() == ImagePlus.GRAY16 )
					result = performStitching( image1, getImage( imp2, roi2, roiZ2, imgFactoryShort, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else if ( imp2.getType() == ImagePlus.GRAY8 )
					result = performStitching( image1, getImage( imp2, roi2, roiZ2, imgFactoryByte, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else
					Log.error( "Unknown image type: " + imp2.getType() );
			}
			else if ( imp1.getType() == ImagePlus.GRAY8 )
			{
				final Image< UnsignedByteType > image1 = getImage( imp1, roi1, roiZ1, imgFactoryByte, params.channel1, timepoint1 );

				if ( imp2.getType() == ImagePlus.GRAY32 )
					result = performStitching( image1, getImage( imp2, roi2, roiZ2, imgFactoryFloat, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else if ( imp2.getType() == ImagePlus.GRAY16 )
					result = performStitching( image1, getImage( imp2, roi2, roiZ2, imgFactoryShort, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else if ( imp2.getType() == ImagePlus.GRAY8 )
					result = performStitching( image1, getImage( imp2, roi2, roiZ2, imgFactoryByte, params.channel2, timepoint2 ), params, numHighestPeaks, searchRadiusPointValidator, roiToTileOffset, globalOffset, stageOffset, confidenceIntervalMin, confidenceIntervalMax );
				else
					Log.error( "Unknown image type: " + imp2.getType() );
			}
			else
			{
				Log.error( "Unknown image type: " + imp1.getType() );
			}
		}
		if ( result == null )
		{
			Log.error( "Pairwise stitching failed." );
			return null;
		}

		// add the offset to the shift
		for ( int i = 0; i < result.length; i++ )
		{
			if ( result[ i ] != null )
			{
				if ( roi2 != null )
				{
					result[ i ].getOffset()[ 0 ] -= roi2.getBounds().x;
					result[ i ].getOffset()[ 1 ] -= roi2.getBounds().y;
				}

				if ( roi1 != null )
				{
					result[ i ].getOffset()[ 0 ] += roi1.getBounds().x;
					result[ i ].getOffset()[ 1 ] += roi1.getBounds().y;
				}

				if ( roiZ2 != null )
					result[ i ].getOffset()[ 2 ] -= roiZ2[ 0 ];

				if ( roiZ1 != null )
					result[ i ].getOffset()[ 2 ] += roiZ1[ 0 ];
			}
		}

		return result;
	}

	public static < T extends RealType<T>, S extends RealType<S> > SerializablePairWiseStitchingResult[] performStitching(
			final Image<T> img1, final Image<S> img2, final StitchingParameters params, final int numHighestPeaks,
			final PointValidator searchRadiusPointValidator, final long[][] roiToTileOffset, final double[] globalOffset,
			final double[] stageOffset, final long[] confidenceIntervalMin, final long[] confidenceIntervalMax )
	{
		if ( img1 == null )
		{
			Log.error( "Image 1 could not be wrapped." );
			return null;
		}
		else if ( img2 == null )
		{
			Log.error( "Image 2 could not be wrapped." );
			return null;
		}
		else if ( params == null )
		{
			Log.error( "Parameters are null." );
			return null;
		}

		final SerializablePairWiseStitchingResult[] result = computePhaseCorrelation(
				img1, img2, params.checkPeaks, params.subpixelAccuracy, numHighestPeaks,
				searchRadiusPointValidator, roiToTileOffset, globalOffset,
				stageOffset, confidenceIntervalMin, confidenceIntervalMax );

		return result;
	}

	public static < T extends RealType<T>, S extends RealType<S> > SerializablePairWiseStitchingResult[] computePhaseCorrelation(
			final Image<T> img1, final Image<S> img2, final int numPeaks, final boolean subpixelAccuracy, final int numHighestPeaks,
			final PointValidator searchRadiusPointValidator, final long[][] roiToTileOffset, final double[] globalOffset,
			final double[] stageOffset, final long[] confidenceIntervalMin, final long[] confidenceIntervalMax )
	{
		final PhaseCorrelation< T, S > phaseCorr = new PhaseCorrelation<>( img1, img2 );
		phaseCorr.setInvestigateNumPeaks( numPeaks );

		phaseCorr.setSearchRadiusPointValidator( searchRadiusPointValidator );
		phaseCorr.setRoiToTileOffset( roiToTileOffset );
		phaseCorr.setGlobalOffset( globalOffset );
		phaseCorr.setStageOffset( stageOffset );
		phaseCorr.setConfidenceInterval( confidenceIntervalMin, confidenceIntervalMax );

		if ( numThreads > 0 )
		{
			phaseCorr.setNumThreads( numThreads );
			phaseCorr.setComputeFFTinParalell( numThreads != 1 );
		}

		if ( subpixelAccuracy )
			phaseCorr.setKeepPhaseCorrelationMatrix( true );

		if ( !phaseCorr.process() )
		{
			Log.error( "Could not compute phase correlation: " + phaseCorr.getErrorMessage() );
			return null;
		}

		// multiple peaks result
		final List< PhaseCorrelationPeak > peaks = phaseCorr.getAllShifts();
		final float[] shift = new float[ img1.getNumDimensions() ];
		final SerializablePairWiseStitchingResult[] result = new SerializablePairWiseStitchingResult[ numHighestPeaks ];

		System.out.println( "Obtaining " + numHighestPeaks + " highest peaks..." );

		for ( int index = 0; index < result.length; index++ )
		{
			final int peakSortedDescIndex = peaks.size() - index - 1;
			if ( peakSortedDescIndex < 0 )
			{
				result[ index ] = null;
			}
			else
			{
				final PhaseCorrelationPeak pcp = peaks.get( peakSortedDescIndex );

				if ( subpixelAccuracy )
				{
					final Image<FloatType> pcm = phaseCorr.getPhaseCorrelationMatrix();

					final ArrayList<DifferenceOfGaussianPeak<FloatType>> list = new ArrayList<>();
					final Peak p = new Peak( pcp );
					list.add( p );

					final SubpixelLocalization<FloatType> spl = new SubpixelLocalization<>( pcm, list );
					final boolean move[] = new boolean[ pcm.getNumDimensions() ];
					for ( int i = 0; i < pcm.getNumDimensions(); ++i )
						move[ i ] = false;
					spl.setCanMoveOutside( true );
					spl.setAllowedToMoveInDim( move );
					spl.setMaxNumMoves( 0 );
					spl.setAllowMaximaTolerance( false );
					spl.process();

					final Peak peak = (Peak)list.get( 0 );

					for ( int d = 0; d < img1.getNumDimensions(); ++d )
						shift[ d ] = peak.getPCPeak().getPosition()[ d ] + peak.getSubPixelPositionOffset( d );

					result[ index ] = new SerializablePairWiseStitchingResult( null, shift.clone(), pcp.getCrossCorrelationPeak(), p.getValue().get() );
				}
				else
				{
					for ( int d = 0; d < img1.getNumDimensions(); ++d )
						shift[ d ] = pcp.getPosition()[ d ];

					result[ index ] = new SerializablePairWiseStitchingResult( null, shift.clone(), pcp.getCrossCorrelationPeak(), pcp.getPhaseCorrelationPeak() );
				}
			}
		}

		if ( subpixelAccuracy )
			phaseCorr.getPhaseCorrelationMatrix().close();

		return result;

		// result
		/*final PhaseCorrelationPeak pcp = phaseCorr.getShift();
		final float[] shift = new float[ img1.getNumDimensions() ];
		final PairWiseStitchingResult result;

		if ( subpixelAccuracy )
		{
			final Image<FloatType> pcm = phaseCorr.getPhaseCorrelationMatrix();

			final ArrayList<DifferenceOfGaussianPeak<FloatType>> list = new ArrayList<DifferenceOfGaussianPeak<FloatType>>();
			final Peak p = new Peak( pcp );
			list.add( p );

			final SubpixelLocalization<FloatType> spl = new SubpixelLocalization<FloatType>( pcm, list );
			final boolean move[] = new boolean[ pcm.getNumDimensions() ];
			for ( int i = 0; i < pcm.getNumDimensions(); ++i )
				move[ i ] = false;
			spl.setCanMoveOutside( true );
			spl.setAllowedToMoveInDim( move );
			spl.setMaxNumMoves( 0 );
			spl.setAllowMaximaTolerance( false );
			spl.process();

			final Peak peak = (Peak)list.get( 0 );

			for ( int d = 0; d < img1.getNumDimensions(); ++d )
				shift[ d ] = peak.getPCPeak().getPosition()[ d ] + peak.getSubPixelPositionOffset( d );

			pcm.close();

			result = new PairWiseStitchingResult( shift, pcp.getCrossCorrelationPeak(), p.getValue().get() );
		}
		else
		{
			for ( int d = 0; d < img1.getNumDimensions(); ++d )
				shift[ d ] = pcp.getPosition()[ d ];

			result = new PairWiseStitchingResult( shift, pcp.getCrossCorrelationPeak(), pcp.getPhaseCorrelationPeak() );
		}
		return result;*/


		/*final ArrayList< PhaseCorrelationPeak > allPeaks = phaseCorr.getAllShifts();
		double bestScore = -Double.MAX_VALUE;
		PhaseCorrelationPeak bestPeak = null;



		if ( false ) // subpixelAccuracy
		{
			// TODO
		}
		else
		{
			A: for ( final PhaseCorrelationPeak peak : allPeaks )
			{
				double dist = 0.0, allTileOffset = 0.0;

				final int[] newOffset = peak.getPosition();
				if ( roi2 != null )
				{
					newOffset[ 0 ] -= roi2.getBounds().x;
					newOffset[ 1 ] -= roi2.getBounds().y;
				}

				if ( roi1 != null )
				{
					newOffset[ 0 ] += roi1.getBounds().x;
					newOffset[ 1 ] += roi1.getBounds().y;
				}

				for ( int d = 0; d < initialOffset.length; ++d )
				{
					final double distD = Math.abs( initialOffset[ d ] - newOffset[ d ] );
					final double allTileOffsetD = fullImageDimensions[ d ];

					//if ( distD >= allTileOffsetD )
					if (distD >= 50)
						continue A;  // wrong non-overlapping shift

					dist += distD;
					allTileOffset += allTileOffsetD;
				}

				final double crossCorr = peak.getCrossCorrelationPeak();
				//double score = 1.0 - ( dist / allTileOffset );
				double score = 1.0; // dist == 0 ? 1.0 : 1.0 / dist;
				score *= crossCorr;

				if ( bestScore < score )
				{
					bestScore = score;
					bestPeak = peak;
				}
			}
		}

		final float[] bestShift = new float[ img1.getNumDimensions() ];
		for ( int d = 0; d < img1.getNumDimensions(); ++d )
			bestShift[ d ] = bestPeak.getPosition()[ d ];*/

		/*final double[] howFar = new double[ bestShift.length ];
		for ( int d = 0; d < bestShift.length; d++ )
			howFar[ d ] = initialOffset[ d ] - bestShift[ d ];
		final double[] howFarLast = new double[ bestShift.length ];
		for ( int d = 0; d < bestShift.length; d++ )
			howFarLast[ d ] = initialOffset[ d ] - phaseCorr.getShift().getPosition()[ d ];
		System.out.println( "******* New one: " + Arrays.toString( howFar ) + ", cr.corr="+bestPeak.getCrossCorrelationPeak() );
		System.out.println( "******* Last: " + Arrays.toString( howFarLast ) + ", cr.corr=" + phaseCorr.getShift().getCrossCorrelationPeak());*/

		//return new PairWiseStitchingResult( bestShift, bestPeak.getCrossCorrelationPeak(), bestPeak.getPhaseCorrelationPeak() );
	}

	/**
	 * return an {@link Image}<T> as input for the PhaseCorrelation.
	 *
	 * @param imp - the {@link ImagePlus}
	 * @param imgFactory - the {@link ImageFactory} defining wher to put it into
	 * @param channel - which channel (if channel=0 means average all channels)
	 * @param timepoint - which timepoint
	 *
	 * @return - the {@link Image} or null if it was not an ImagePlus.GRAY8, ImagePlus.GRAY16 or ImagePlus.GRAY32
	 */
	public static < T extends RealType<T> > Image<T> getImage( final ImagePlus imp, Roi roi, final int[] roiZ, final ImageFactory<T> imgFactory, final int channel, final int timepoint )
	{
		// first test the roi
		roi = getOnlyRectangularRoi( roi );

		// how many dimensions?
		final int numDimensions;
		if ( imp.getNSlices() > 1 )
			numDimensions = 3;
		else
			numDimensions = 2;

		// the size of the image
		final int[] size = new int[ numDimensions ];
		final int[] offset = new int[ numDimensions ];

		if ( roi == null )
		{
			size[ 0 ] = imp.getWidth();
			size[ 1 ] = imp.getHeight();
		}
		else
		{
			size[ 0 ] = roi.getBounds().width;
			size[ 1 ] = roi.getBounds().height;

			offset[ 0 ] = roi.getBounds().x;
			offset[ 1 ] = roi.getBounds().y;
		}

		if ( numDimensions == 3 )
		{
			if ( roiZ == null)
			{
				size[ 2 ] = imp.getNSlices();
			}
			else
			{
				offset[ 2 ] = roiZ[ 0 ];
				size[ 2 ] = roiZ[ 1 ];
			}
		}

		System.out.println( "Cropping input image with ROI of " + Arrays.toString( size ) + " at " + Arrays.toString( offset ) );

		// create the Image
		final Image<T> img = imgFactory.createImage( size );
		final boolean success;

		// copy the content
		if ( channel == 0 )
		{
			// we need to average all channels
			success = averageAllChannels( img, offset, imp, timepoint );
		}
		else
		{
			// otherwise only copy one channel
			success = fillInChannel( img, offset, imp, channel, timepoint );
		}

		if ( success )
		{
			return img;
		}
		img.close();
		return null;
	}

	/**
	 * Averages all channels into the target image. The size is given by the dimensions of the target image,
	 * the offset (if applicable) is given by an extra field
	 *
	 * @param target - the target Image
	 * @param offset - the offset of the area (might be [0,0] or [0,0,0])
	 * @param imp - the input ImagePlus
	 * @param timepoint - for which timepoint
	 *
	 * @return true if successful, false if the ImagePlus type was unknow
	 */
	public static < T extends RealType< T > > boolean averageAllChannels( final Image< T > target, final int[] offset, final ImagePlus imp, final int timepoint )
	{
		final int numChannels = imp.getNChannels();

		if ( imp.getType() == ImagePlus.GRAY8 )
		{
			final ArrayList< Image< UnsignedByteType > > images = new ArrayList<>();

			// first get wrapped instances of all channels
			for ( int c = 1; c <= numChannels; ++c )
				images.add( getWrappedImageUnsignedByte( imp, c, timepoint ) );

			averageAllChannels( target, images, offset );
			return true;
		}
		else if ( imp.getType() == ImagePlus.GRAY16 )
		{
			final ArrayList< Image< UnsignedShortType > > images = new ArrayList<>();

			// first get wrapped instances of all channels
			for ( int c = 1; c <= numChannels; ++c )
				images.add( getWrappedImageUnsignedShort( imp, c, timepoint ) );

			averageAllChannels( target, images, offset );
			return true;
		}
		else if ( imp.getType() == ImagePlus.GRAY32 )
		{
			final ArrayList< Image< FloatType > > images = new ArrayList<>();

			// first get wrapped instances of all channels
			for ( int c = 1; c <= numChannels; ++c )
				images.add( getWrappedImageFloat( imp, c, timepoint ) );

			averageAllChannels( target, images, offset );
			return true;
		}
		else
		{
			Log.error( "Unknow image type: " + imp.getType() );
			return false;
		}
	}

	/**
	 * Averages all channels into the target image. The size is given by the dimensions of the target image,
	 * the offset (if applicable) is given by an extra field
	 *
	 * @param target - the target Image
	 * @param offset - the offset of the area (might be [0,0] or [0,0,0])
	 * @param imp - the input ImagePlus
	 * @param timepoint - for which timepoint
	 *
	 * @return true if successful, false if the ImagePlus type was unknow
	 */
	public static < T extends RealType< T > > boolean fillInChannel( final Image< T > target, final int[] offset, final ImagePlus imp, final int channel, final int timepoint )
	{
		if ( imp.getType() == ImagePlus.GRAY8 )
		{
			final ArrayList< Image< UnsignedByteType > > images = new ArrayList<>();

			// first get wrapped instances of all channels
			images.add( getWrappedImageUnsignedByte( imp, channel, timepoint ) );

			averageAllChannels( target, images, offset );
			return true;
		}
		else if ( imp.getType() == ImagePlus.GRAY16 )
		{
			final ArrayList< Image< UnsignedShortType > > images = new ArrayList<>();

			// first get wrapped instances of all channels
			images.add( getWrappedImageUnsignedShort( imp, channel, timepoint ) );

			averageAllChannels( target, images, offset );
			return true;
		}
		else if ( imp.getType() == ImagePlus.GRAY32 )
		{
			final ArrayList< Image< FloatType > > images = new ArrayList<>();

			// first get wrapped instances of all channels
			images.add( getWrappedImageFloat( imp, channel, timepoint ) );

			averageAllChannels( target, images, offset );
			return true;
		}
		else
		{
			Log.error( "Unknow image type: " + imp.getType() );
			return false;
		}
	}

	/**
	 * Averages all channels into the target image. The size is given by the dimensions of the target image,
	 * the offset (if applicable) is given by an extra field
	 *
	 * @param target - the target Image
	 * @param offset - the offset of the area (might be [0,0] or [0,0,0])
	 * @param sources - a list of input Images
	 */
	protected static < T extends RealType< T >, S extends RealType< S > > void averageAllChannels( final Image< T > target, final ArrayList< Image< S > > sources, final int[] offset )
	{
		// get the major numbers
		final int numDimensions = target.getNumDimensions();
		final float numImages = sources.size();
		long imageSize = target.getDimension( 0 );

		for ( int d = 1; d < target.getNumDimensions(); ++d )
			imageSize *= target.getDimension( d );

		// run multithreaded
		final AtomicInteger ai = new AtomicInteger(0);
		Thread[] threads;
		if ( numThreads > 0 )
			threads = SimpleMultiThreading.newThreads( numThreads );
		else
			threads = SimpleMultiThreading.newThreads();

		final Vector<Chunk> threadChunks = SimpleMultiThreading.divideIntoChunks( imageSize, threads.length );

		for (int ithread = 0; ithread < threads.length; ++ithread)
			threads[ithread] = new Thread(new Runnable()
			{
				@Override
				public void run()
				{
					// Thread ID
					final int myNumber = ai.getAndIncrement();

					// get chunk of pixels to process
					final Chunk myChunk = threadChunks.get( myNumber );
					final long startPos = myChunk.getStartPosition();
					final long loopSize = myChunk.getLoopSize();

					// the cursor for the output
					final LocalizableCursor< T > targetCursor =  target.createLocalizableCursor();

					// the input cursors
					final ArrayList< LocalizableByDimCursor< S > > sourceCursors = new ArrayList< > ();

					for ( final Image< S > source : sources )
						sourceCursors.add( source.createLocalizableByDimCursor() );

					// temporary array
					final int[] location = new int[ numDimensions ];

					// move to the starting position of the current thread
					targetCursor.fwd( startPos );

					// do as many pixels as wanted by this thread
					for ( long j = 0; j < loopSize; ++j )
					{
						targetCursor.fwd();
						targetCursor.getPosition( location );

						for ( int d = 0; d < numDimensions; ++d )
							location[ d ] += offset[ d ];

						float sum = 0;

						for ( final LocalizableByDimCursor< S > sourceCursor : sourceCursors )
						{
							sourceCursor.setPosition( location );
							sum += sourceCursor.getType().getRealFloat();
						}

						targetCursor.getType().setReal( sum / numImages );
					}
				}
			});

		SimpleMultiThreading.startAndJoin( threads );
	}

	/**
	 * return an {@link Image} of {@link UnsignedByteType} as input for the PhaseCorrelation. If no rectangular roi
	 * is selected, it will only wrap the existing ImagePlus!
	 *
	 * @param targetType - which {@link RealType}
	 * @param channel - which channel (if channel=0 means average all channels)
	 * @param timepoint - which timepoint
	 * @param imp - the {@link ImagePlus}
	 *
	 * @return - the {@link Image} or null if it was not an ImagePlus.GRAY8 or if channel = 0
	 */
	public static Image<UnsignedByteType> getWrappedImageUnsignedByte( final ImagePlus imp, final int channel, final int timepoint )
	{
		if ( channel == 0 || imp.getType() != ImagePlus.GRAY8 )
			return null;
		return ImageJFunctions.wrapByte( Hyperstack_rearranger.getImageChunk( imp, channel, timepoint ) );
	}

	/**
	 * return an {@link Image} of {@link UnsignedShortType} as input for the PhaseCorrelation. If no rectangular roi
	 * is selected, it will only wrap the existing ImagePlus!
	 *
	 * @param targetType - which {@link RealType}
	 * @param channel - which channel (if channel=0 means average all channels)
	 * @param timepoint - which timepoint
	 * @param imp - the {@link ImagePlus}
	 *
	 * @return - the {@link Image} or null if it was not an ImagePlus.GRAY16 or if channel = 0
	 */
	public static Image<UnsignedShortType> getWrappedImageUnsignedShort( final ImagePlus imp, final int channel, final int timepoint )
	{
		if ( channel == 0 || imp.getType() != ImagePlus.GRAY16 )
			return null;
		return ImageJFunctions.wrapShort( Hyperstack_rearranger.getImageChunk( imp, channel, timepoint ) );
	}

	/**
	 * return an {@link Image} of {@link FloatType} as input for the PhaseCorrelation. If no rectangular roi
	 * is selected, it will only wrap the existing ImagePlus!
	 *
	 * @param targetType - which {@link RealType}
	 * @param channel - which channel (if channel=0 means average all channels)
	 * @param timepoint - which timepoint
	 * @param imp - the {@link ImagePlus}
	 *
	 * @return - the {@link Image} or null if it was not an ImagePlus.GRAY32 or if channel = 0
	 */
	public static Image<FloatType> getWrappedImageFloat( final ImagePlus imp, final int channel, final int timepoint )
	{
		if ( channel == 0 || imp.getType() != ImagePlus.GRAY32 )
			return null;
		return ImageJFunctions.wrapFloat( Hyperstack_rearranger.getImageChunk( imp, channel, timepoint ) );
	}

	/**
	 * Determines if this imageplus with these parameters can be wrapped directly into an Image<T>.
	 * This is important, because if we would wrap the first but not the second image, they would
	 * have different {@link ImageFactory}s
	 *
	 * @param imp - the ImagePlus
	 * @param channel - which channel (if channel=0 means average all channels)
	 *
	 * @return true if it can be wrapped, otherwise false
	 */
	public static boolean canWrapIntoImgLib( final ImagePlus imp, Roi roi, final int channel )
	{
		// first test the roi
		roi = getOnlyRectangularRoi( roi );

		return roi == null && channel > 0;
	}

	protected static Roi getOnlyRectangularRoi( final Roi roi )
	{
		// we can only do rectangular rois
		if ( roi != null && roi.getType() != Roi.RECTANGLE )
			return null;
		return roi;
	}

	/*
	protected static Roi getOnlyRectangularRoi( final ImagePlus imp )
	{
		Roi roi = imp.getRoi();

		// we can only do rectangular rois
		if ( roi != null && roi.getType() != Roi.RECTANGLE )
		{
			Log.warn( "roi for " + imp.getTitle() + " is not a rectangle, we have to ignore it." );
			roi = null;
		}

		return roi;
	}
	 */
}
