package org.janelia.stitching.analysis;

import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class CompareEstimatedShiftsConstrainedVsUnconstrained
{
	private static class CompareEstimatedShiftsCmdArgs implements Serializable
	{
		private static final long serialVersionUID = 215043103837732209L;

		@Option(name = "-c", aliases = { "--constrainedPairwisePath" }, required = true,
				usage = "Path to the constrained pairwise configuration file.")
		public String constrainedPairwisePath;

		@Option(name = "-uc", aliases = { "--unconstrainedPairwisePath" }, required = true,
				usage = "Path to the unconstrained pairwise configuration file.")
		public String unconstrainedPairwisePath;

		public boolean parsedSuccessfully = false;

		public CompareEstimatedShiftsCmdArgs( final String... args ) throws IllegalArgumentException
		{
			final CmdLineParser parser = new CmdLineParser( this );
			try
			{
				parser.parseArgument( args );
				parsedSuccessfully = true;
			}
			catch ( final CmdLineException e )
			{
				System.err.println( e.getMessage() );
				parser.printUsage( System.err );
			}
		}
	}

	public static void main( final String[] args ) throws Exception
	{
		final CompareEstimatedShiftsCmdArgs parsedArgs = new CompareEstimatedShiftsCmdArgs( args );
		if ( !parsedArgs.parsedSuccessfully )
			System.exit( 1 );

		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final List< SerializablePairWiseStitchingResult > constrainedPairwiseShifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( parsedArgs.constrainedPairwisePath ) ) );
		final List< SerializablePairWiseStitchingResult > unconstrainedPairwiseShifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( parsedArgs.unconstrainedPairwisePath ) ) );
		System.out.println( String.format(
				"Num total pairs: constrained=%d, unconstrained=%d",
				constrainedPairwiseShifts.size(),
				unconstrainedPairwiseShifts.size()
			) );

		final Map< Integer, ? extends Map< Integer, SerializablePairWiseStitchingResult > > constrainedPairwiseShiftsMap = Utils.createSubTilePairwiseResultsMap( constrainedPairwiseShifts, false );
		final Map< Integer, ? extends Map< Integer, SerializablePairWiseStitchingResult > > unconstrainedPairwiseShiftsMap = Utils.createSubTilePairwiseResultsMap( unconstrainedPairwiseShifts, false );
		System.out.println( String.format(
				"Num extra pairs: constrained=%d, unconstrained=%d",
				countExtraPairsInA( constrainedPairwiseShiftsMap, unconstrainedPairwiseShiftsMap ),
				countExtraPairsInA( unconstrainedPairwiseShiftsMap, constrainedPairwiseShiftsMap )
			) );


		try ( final PrintWriter fileWriter = new PrintWriter( "constrained-vs-unconstrained.txt" ) )
		{
			fileWriter.println( "constrained_cr.corr unconstrained_cr.corr constrained_ph.corr unconstrained_ph.corr" );
			for ( final Entry< Integer, ? extends Map< Integer, SerializablePairWiseStitchingResult > > constrainedPairwiseShiftOuterEntry : constrainedPairwiseShiftsMap.entrySet() )
			{
				for ( final Entry< Integer, SerializablePairWiseStitchingResult > constrainedPairwiseShiftInnerEntry : constrainedPairwiseShiftOuterEntry.getValue().entrySet() )
				{
					final SerializablePairWiseStitchingResult constrainedPairwiseShift = constrainedPairwiseShiftInnerEntry.getValue();
					final SerializablePairWiseStitchingResult unconstrainedPairwiseShift = unconstrainedPairwiseShiftsMap.get( constrainedPairwiseShiftOuterEntry.getKey() ).get( constrainedPairwiseShiftInnerEntry.getKey() );
					fileWriter.println( String.format(
							"%.5f %.5f %.5f %.5f",
							constrainedPairwiseShift.getCrossCorrelation(), unconstrainedPairwiseShift.getCrossCorrelation(),
							constrainedPairwiseShift.getPhaseCorrelation(), unconstrainedPairwiseShift.getPhaseCorrelation()
						) );
				}
			}
		}

		System.out.println( "Done" );
	}

	private static int countExtraPairsInA(
			final Map< Integer, ? extends Map< Integer, SerializablePairWiseStitchingResult > > a,
			final Map< Integer, ? extends Map< Integer, SerializablePairWiseStitchingResult > > b )
	{
		int numExtraPairs = 0;
		for ( final Entry< Integer, ? extends Map< Integer, SerializablePairWiseStitchingResult > > outerEntry : a.entrySet() )
			for ( final Entry< Integer, SerializablePairWiseStitchingResult > innerEntry : outerEntry.getValue().entrySet() )
				if ( !b.containsKey( outerEntry.getKey() ) || !b.get( outerEntry.getKey() ).containsKey( innerEntry.getKey() ) )
					++numExtraPairs;
		return numExtraPairs;
	}
}
