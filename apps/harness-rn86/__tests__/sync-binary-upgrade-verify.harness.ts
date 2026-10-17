import { test } from 'react-native-harness';
import { runBinaryUpgradePhase } from '../../harness-shared/sync-binary-upgrade-fixture';
test('reopens the task11 journal using the task14 binary', () =>
  runBinaryUpgradePhase('verify'));
